# Architecture

This document describes the technical implementation of the NATS JetStream transport for NServiceBus.

## Overview

The transport implements the NServiceBus `TransportDefinition` abstraction using NATS JetStream for durable messaging. JetStream provides at-least-once delivery guarantees with message persistence and replay capabilities.

## Key Components

### NatsTransport

The entry point that extends `TransportDefinition`. Configures transport settings and creates the infrastructure.

```
NatsTransport : TransportDefinition
├── Initialize() → NatsTransportInfrastructure
└── Configuration properties (StreamPrefix, AckWait, etc.)
```

### NatsTransportInfrastructure

Creates receivers and dispatchers, manages the NATS connection lifecycle.

```
NatsTransportInfrastructure : TransportInfrastructure
├── Receivers[] → MessagePump instances
├── Dispatcher → MessageDispatcher
├── ConnectionManager → NatsConnectionManager
└── TopologyManager → Stream/consumer management
```

### MessagePump

Implements `IMessageReceiver` for consuming messages from JetStream. Each endpoint has one pump that handles both:

- **Queue messages**: Direct sends to the endpoint (unicast)
- **Event messages**: Pub/sub events the endpoint subscribes to

```
MessagePump : IMessageReceiver
├── ReceiveFromQueue() → Consumes from endpoint stream
├── ReceiveFromEvents() → Consumes from events stream with filters
├── ProcessMessage() → Invokes NServiceBus pipeline
└── Concurrency control via SemaphoreSlim
```

### MessageDispatcher

Implements `IMessageDispatcher` for sending messages to JetStream.

```
MessageDispatcher : IMessageDispatcher
├── SendUnicast() → Publish to endpoint subject
├── SendMulticast() → Publish to type hierarchy subjects
└── SendDelayed() → Publish to delayed stream
```

### TopologyManager

Manages JetStream streams and consumers.

```
TopologyManager
├── CreateEndpointInfrastructure() → Endpoint stream + consumer
├── CreateEventsInfrastructure() → Shared events stream
├── CreateDelayedDeliveryInfrastructure() → Delayed messages stream
├── SubscribeToEvent() → Update consumer filter subjects
└── UnsubscribeFromEvent() → Update consumer filter subjects
```

### DelayedDeliveryProcessor

Background worker that polls the delayed stream and forwards messages when their delivery time arrives.

```
DelayedDeliveryProcessor
├── ProcessDelayedMessages() → Polling loop
├── ProcessDelayedMessage() → Check time, forward or NAK
└── Forward to destination (unicast or multicast)
```

## Stream Topology

The transport creates three types of JetStream streams:

### Endpoint Streams

One stream per endpoint for direct (unicast) messaging.

```
Stream: {prefix}-{endpoint}
Subject: {prefix}.endpoint.{endpoint}
Retention: WorkQueue (delete after ack)
Consumer: {endpoint}-main (durable, explicit ack)
```

### Events Stream

Shared stream for all pub/sub events.

```
Stream: {prefix}-events
Subject: {prefix}.events.>
Retention: Limits (time/count based)
Consumers: {endpoint}-events per subscribing endpoint
           with FilterSubjects for subscribed types
```

### Delayed Stream

Stores messages for future delivery.

```
Stream: {prefix}-delayed
Subject: {prefix}.delayed.>
Retention: WorkQueue
Consumer: delayed-processor (single, shared)
```

## Message Flow

### Send (Unicast)

```
Sender → MessageDispatcher.SendUnicast()
       → jetStream.PublishAsync(subject: "{prefix}.endpoint.{destination}")
       → Endpoint Stream
       → MessagePump.ReceiveFromQueue()
       → NServiceBus Pipeline
       → msg.AckAsync()
```

### Publish (Multicast)

```
Publisher → MessageDispatcher.SendMulticast()
          → GetTypeHierarchy(messageType) → [ConcreteType, BaseClass, IInterface]
          → For each type:
              jetStream.PublishAsync(subject: "{prefix}.events.{type}")
          → Events Stream
          → Subscribers with matching FilterSubjects
          → MessagePump.ReceiveFromEvents()
          → NServiceBus Pipeline
          → msg.AckAsync()
```

### Delayed Delivery

```
Sender → MessageDispatcher.SendDelayed()
       → Add headers: DeliveryAt, Destination, IsMulticast
       → jetStream.PublishAsync(subject: "{prefix}.delayed.{destination}")
       → Delayed Stream

DelayedDeliveryProcessor (polling):
       → FetchAsync from delayed consumer
       → If DeliveryAt <= now:
           Forward to destination (unicast or multicast)
           msg.AckAsync()
       → Else:
           msg.NakAsync(delay: min(timeUntilDelivery, 30s))
```

## Polymorphic Event Publishing

When publishing an event, the transport publishes to subjects for the entire type hierarchy:

```csharp
class OrderPlaced : OrderEvent, IOrderEvent { }

// Publishes to:
// - {prefix}.events.MyApp-OrderPlaced
// - {prefix}.events.MyApp-OrderEvent
// - {prefix}.events.MyApp-IOrderEvent
```

Each publish uses a unique `Nats-Msg-Id` per subject to avoid JetStream deduplication:
```
{messageId}-{sanitized-type-name}
```

The `NServiceBus.MessageId` header remains the same across all publishes for Outbox deduplication.

## Subscription Management

Subscriptions are managed by updating the consumer's `FilterSubjects`:

```csharp
// Subscribe to OrderPlaced
await topologyManager.SubscribeToEvent("MyEndpoint", "MyApp.OrderPlaced");

// Consumer config updated:
// FilterSubjects: ["nsb.events.MyApp-OrderPlaced"]

// Subscribe to another event
await topologyManager.SubscribeToEvent("MyEndpoint", "MyApp.OrderCancelled");

// Consumer config updated:
// FilterSubjects: ["nsb.events.MyApp-OrderPlaced", "nsb.events.MyApp-OrderCancelled"]
```

When subscriptions change, the MessagePump restarts its events consumer to pick up the new filters.

## Concurrency Control

Message processing concurrency is controlled by a `SemaphoreSlim`:

```csharp
// Before processing
await concurrencyLimiter.WaitAsync(cancellationToken);
Interlocked.Increment(ref messagesBeingProcessed);

// Process message (fire and forget for concurrency)
_ = ProcessMessageWithConcurrencyTracking(msg, token);

// After processing (in finally block)
concurrencyLimiter.Release();
Interlocked.Decrement(ref messagesBeingProcessed);
```

Dynamic concurrency changes stop and restart the pump with new limits.

## Error Handling

### Message Processing Errors

1. Exception thrown during processing
2. `onError` callback invoked (NServiceBus recoverability)
3. If `RetryRequired`: `msg.NakAsync(delay: 1s)` for redelivery
4. If handled: `msg.AckAsync()` (moved to error queue by NServiceBus)

### Connection Errors

1. Circuit breaker tracks consecutive failures
2. Exponential backoff with configurable timeout
3. If timeout exceeded: critical error callback invoked
4. NATS.Net handles reconnection automatically

## Header Encoding

NATS headers are ASCII-only. Non-ASCII characters are encoded using MIME-style Base64:

```
Original: "Ключ"
Encoded:  "=?UTF-8?B?0JrQu9GO0Yc=?="
```

Both keys and values are encoded/decoded transparently.

## Subject Naming

Type names are sanitized for NATS subjects:

- `.` (namespace separator) → `-`
- `+` (nested type) → `--`

Example: `MyApp.Orders+OrderPlaced` → `MyApp-Orders--OrderPlaced`

This avoids conflicts with NATS wildcards (`.` is a token separator, `+` is a single-token wildcard).
