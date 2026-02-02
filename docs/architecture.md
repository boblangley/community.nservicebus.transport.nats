# Architecture

This document describes how the NServiceBus NATS transport implements NServiceBus messaging capabilities using NATS JetStream features.

## Requirements

- **NATS Server 2.12+** - Required for native message scheduling (ADR-51)
- **JetStream enabled** - Required for durable messaging

## NServiceBus Capabilities

### Unicast Messaging (Send)

**NServiceBus capability**: Send a message to a specific endpoint.

**NATS implementation**: Each endpoint has a dedicated JetStream stream that captures messages published to its subject.

| Component | Implementation |
|-----------|----------------|
| Stream | `{prefix}-{endpoint}` with WorkQueue retention |
| Subject | `{prefix}.endpoint.{endpoint}` |
| Consumer | Durable consumer with explicit ack |
| Delivery | At-least-once via JetStream ack/nak |

**Message flow**:
```
Sender → publish to subject → Stream captures → Consumer delivers → Endpoint processes → Ack
```

**Key code**: `MessageDispatcher.SendUnicast()` publishes to the endpoint subject. If the destination stream doesn't exist, it's created lazily via `TopologyManager.EnsureEndpointStreamExists()`.

### Publish/Subscribe

**NServiceBus capability**: Publish an event to all interested subscribers.

**NATS implementation**: Uses a central events stream with JetStream sourcing. Events are published to the central stream, and endpoint streams source from it with filters for their subscribed event types.

| Component | Implementation |
|-----------|----------------|
| Central Stream | `{prefix}-events` captures all `{prefix}.events.>` subjects |
| Subject | `{prefix}.events.{event-type}` |
| Sourcing | Endpoint streams source from events stream with filter subjects |
| Consumer | `{endpoint}-main` consumer handles unicast and sourced events |

**How it works**:
1. Central events stream created at startup, captures all `{prefix}.events.>` subjects
2. Endpoint subscribes to event type (e.g., `OrderPlaced`)
3. `TopologyManager.SubscribeToEvent()` adds a Source to endpoint stream with filter `{prefix}.events.MyApp-OrderPlaced`
4. Publisher calls `MessageDispatcher.SendMulticast()` which publishes to `{prefix}.events.MyApp-OrderPlaced`
5. JetStream sourcing automatically copies matching messages to endpoint streams
6. Single consumer loop delivers the event for processing

**Polymorphic publishing**: Events are published to separate subjects for each type in the hierarchy, with unique message IDs per type:

```
class OrderPlaced : OrderEvent, IOrderEvent { }

// Published to three subjects with unique IDs:
// - {prefix}.events.MyApp-OrderPlaced (ID: {msgId}-MyApp-OrderPlaced)
// - {prefix}.events.MyApp-OrderEvent (ID: {msgId}-MyApp-OrderEvent)
// - {prefix}.events.MyApp-IOrderEvent (ID: {msgId}-MyApp-IOrderEvent)
```

**Note**: An endpoint subscribing to multiple types in the same hierarchy will receive multiple copies of the message. This is consistent with other message broker behavior.

**Subscription management**: When an endpoint subscribes to an event type, `TopologyManager.SubscribeToEvent()` adds a Source from the events stream with the appropriate filter subject.

**Benefits**:
- Single consumer per endpoint simplifies concurrency control
- Single queue depth metric for autoscaling
- WorkQueue retention automatically cleans up processed events
- Horizontal scaling via shared durable consumer

**Key code**:
- `MessageDispatcher.SendMulticast()` - publishes to type hierarchy subjects with unique IDs
- `TopologyManager.SubscribeToEvent()` - adds source filter to endpoint stream
- `MessagePump.ReceiveMessages()` - single loop handles all message types

### Delayed Delivery

**NServiceBus capability**: Deliver a message at a future time (used for saga timeouts, delayed retries).

**NATS implementation**: Uses a central delayed stream with native NATS message scheduling (ADR-51). Scheduled messages are delivered to a "ready" subject, which is sourced by endpoint streams.

| Header | Purpose |
|--------|---------|
| `Nats-Schedule` | `@at {RFC3339 timestamp}` - when to deliver |
| `Nats-Schedule-Target` | Ready subject for delivery |

**Architecture**:
```
{prefix}-delayed stream (central)
  Subjects: {prefix}.delayed.>, {prefix}.ready.>
  AllowMsgSchedules: true

{prefix}-{endpoint} stream (per-endpoint)
  Sources from delayed stream with filter: {prefix}.ready.{endpoint}
```

**How it works**:
1. Message published to `{prefix}.delayed.{endpoint}.{message-id}` in central delayed stream
2. NATS server holds the message until the scheduled time (native ADR-51 scheduling)
3. At delivery time, NATS moves message to `{prefix}.ready.{endpoint}` (same stream)
4. Endpoint stream sources from delayed stream with filter `{prefix}.ready.{endpoint}`
5. Sourcing automatically copies the ready message to the endpoint stream
6. Consumer delivers the message for processing

**Why central delayed stream**: JetStream doesn't allow streams to have both `Sources` and `AllowMsgSchedules`. Using a central delayed stream allows endpoint streams to use Sources for both events and delayed messages, while the central stream handles scheduling.

**Horizontal scaling**: All endpoint instances share the same durable consumer. When a scheduled message becomes ready, sourcing copies it to the endpoint stream, and any instance can process it.

**Key code**: `MessageDispatcher.SendDelayed()` publishes to the delayed stream with native scheduling headers and the ready subject as target.

### Recoverability

**NServiceBus capability**: Retry failed messages with configurable policies.

**How NServiceBus recoverability works**: When a handler throws an exception, NServiceBus core invokes the transport's `onError` callback. The recoverability policy determines the action and NServiceBus core handles message dispatch for delayed retries and error queue moves. The transport only needs to ACK/NAK the original message based on the callback result.

**Transport responsibilities**:

| `onError` Result | What NServiceBus Core Did | Transport Action |
|------------------|---------------------------|------------------|
| `RetryRequired` | Nothing (immediate retry) | `msg.NakAsync(delay: 1s)` - JetStream redelivers |
| `Handled` | Dispatched delayed retry copy | `msg.AckAsync()` - original removed |
| `Handled` | Dispatched to error queue | `msg.AckAsync()` - original removed |

**Delayed retry flow**:
1. Handler throws exception
2. NServiceBus core creates a **copy** of the message
3. Core dispatches the copy via `MessageDispatcher` with `DelayDeliveryWith` property
4. `SendDelayed()` publishes to central delayed stream with `Nats-Schedule` headers
5. Core returns `ErrorHandleResult.Handled`
6. Transport ACKs the original message
7. NATS delivers the scheduled copy at the retry time via sourcing

**Delivery tracking**: JetStream tracks delivery attempts via `msg.Metadata.NumDelivered`, which is passed to NServiceBus as the delivery count for recoverability decisions.

**Key code**: `MessagePump.ProcessMessage()` handles the ack/nak logic based on `onError` callback results.

### Concurrency Control

**NServiceBus capability**: Limit concurrent message processing per endpoint.

**NATS implementation**: A `SemaphoreSlim` gates message processing. Messages are fetched from JetStream but wait for a concurrency slot before processing.

**Dynamic changes**: `MessagePump.ChangeConcurrency()` stops receiving, updates the semaphore, and restarts with new limits.

**Key code**: `MessagePump.ProcessIncomingMessage()` acquires the semaphore before spawning processing.

### Horizontal Scaleout

**NServiceBus capability**: Run multiple instances of the same endpoint to increase throughput.

**NATS implementation**: JetStream's durable consumer model naturally supports competing consumers. All instances of an endpoint share the same consumer name, and JetStream distributes messages across connected instances.

**How it works**:
1. All instances use the same endpoint name → same stream and consumer name
2. Each instance calls `consumer.ConsumeAsync()` on the shared durable consumer
3. JetStream delivers each message to exactly one instance
4. WorkQueue retention removes messages after ACK

**All message types**: Instances share the `{endpoint}-main` consumer. JetStream load-balances unicast messages, sourced events, and sourced delayed messages across all connected instances. Each event is delivered to one instance of each subscribing endpoint (not all instances).

**No configuration required**: Scaleout works automatically - just deploy more instances with the same endpoint name. NATS handles the distribution.

**Consumer state**: JetStream tracks which messages have been delivered and to whom. If an instance disconnects without ACKing, messages are redelivered to another instance after `AckWait` timeout (default 30s).

### Transactions

**Not supported**: NATS JetStream doesn't support distributed transactions. The transport operates in `ReceiveOnly` mode - messages are acknowledged after processing, but outgoing messages are sent immediately (not transactionally).

## Stream Topology Summary

| Stream | Purpose | Retention | Subjects | Sources |
|--------|---------|-----------|----------|---------|
| `{prefix}-events` | Central events routing | Limits (1hr) | `{prefix}.events.>` | None |
| `{prefix}-delayed` | Central scheduling | Limits (1hr) | `{prefix}.delayed.>`, `{prefix}.ready.>` | None |
| `{prefix}-{endpoint}` | Endpoint messages | WorkQueue | `{prefix}.endpoint.{endpoint}` | Delayed + Events |
| `{prefix}-{endpoint}-error` | Error queue | WorkQueue | `{prefix}.endpoint.{endpoint}-error` | Delayed |

**Stream configuration**:
- **Events stream**: `AllowMsgSchedules = false`, no Sources
- **Delayed stream**: `AllowMsgSchedules = true`, no Sources
- **Endpoint streams**: `AllowMsgSchedules = false`, Sources from delayed and events streams

**Note**: Endpoint streams add Sources dynamically when subscribing to events via `TopologyManager.SubscribeToEvent()`.

## Key Files

| File | Responsibility |
|------|----------------|
| `NatsTransport.cs` | Transport configuration, version checking |
| `TopologyManager.cs` | Stream/consumer CRUD, subscription management (adds sources to streams) |
| `MessageDispatcher.cs` | Send, publish, delayed delivery |
| `MessagePump.cs` | Single consumer loop for all message types |
| `SubscriptionManager.cs` | Track subscriptions, update stream sources via TopologyManager |
| `NatsTransportDiagnostics.cs` | OpenTelemetry instrumentation |

## NATS-Specific Considerations

### Subject Naming

Type names are sanitized for NATS subjects:
- `.` (namespace separator) → `-`
- `+` (nested type) → `--`

Example: `MyApp.Orders+OrderPlaced` → `MyApp-Orders--OrderPlaced`

### Header Encoding

NATS headers are ASCII-only. Non-ASCII characters use MIME-style Base64 encoding:
```
Original: "Schlüssel"
Encoded:  "=?UTF-8?B?U2NobMO8c3NlbA==?="
```

### Message Deduplication

JetStream deduplicates by `Nats-Msg-Id` within the stream's duplicate window (2 minutes). The transport uses:
- **Polymorphic publishes**: `{messageId}-{typeName}` for each type in hierarchy - each type gets its own message
- **Scheduled messages**: `{messageId}-sched-{timestamp}` - allows multiple delayed retries of the same message
- **Error queue messages**: `{messageId}-error-{timestamp}` - prevents dedup with original message

### Connection Resilience

The NATS.Net client handles reconnection automatically. A circuit breaker in `MessagePump` tracks consecutive failures and invokes the critical error callback if the configurable timeout is exceeded.

### JetStream Sourcing

Sourcing is a server-side feature that automatically copies messages from one stream to another with optional filtering. Key characteristics:
- **Automatic**: Happens server-side without client involvement
- **Filter subjects**: Can filter which messages are copied
- **Preserves original subject**: Sourced messages retain their original subject
- **Header added**: `Nats-Stream-Source` header indicates the source stream

**Constraint**: Streams with Sources cannot have `AllowMsgSchedules = true` (mutually exclusive by design). This is why the transport uses separate central streams for events (no scheduling) and delayed (with scheduling).
