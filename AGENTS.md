# AGENTS.md

High-level overview for AI agents working on this repository.

## What This Is

NServiceBus transport implementation for NATS JetStream. Enables NServiceBus messaging over NATS with durable streams, pub/sub, and delayed delivery.

## Repository Structure

```
src/
  Community.NServiceBus.Transport.Nats/        # Main transport library
    NatsTransport.cs                           # Entry point, TransportDefinition
    NatsTransportInfrastructure.cs             # Creates pumps/dispatcher
    Sending/MessageDispatcher.cs               # Sends messages to JetStream
    Receiving/MessagePump.cs                   # Receives messages from JetStream
    Administration/TopologyManager.cs          # Manages streams/consumers
    Administration/SubscriptionManager.cs      # Manages event subscriptions
    DelayedDelivery/DelayedDeliveryProcessor.cs # Forwards delayed messages
  Community.NServiceBus.Transport.Nats.TransportTests/     # Unit/integration tests
  Community.NServiceBus.Transport.Nats.AcceptanceTests/    # End-to-end tests

docs/
  configuration.md    # End-user configuration guide
  operations.md       # Deployment, NATS CLI commands
  architecture.md     # Technical implementation details
```

## Key Concepts

### Stream Topology
- `{prefix}-{endpoint}` - Per-endpoint stream for unicast messages
- `{prefix}-events` - Shared stream for pub/sub events
- `{prefix}-delayed` - Stores messages for future delivery

### Message Flow

1. **Unicast**: Publish to `{prefix}.endpoint.{destination}` → endpoint stream → consumer
2. **Multicast**: Publish to `{prefix}.events.{type}` for each type in hierarchy → events stream → filtered consumers
3. **Delayed**: Store in delayed stream → processor polls → forwards when due

## Running Tests

```bash
# Start NATS (devcontainer has this pre-configured)
docker run -d --name nats -p 4222:4222 nats:2.10-alpine --jetstream

# Run tests
dotnet test src/Community.NServiceBus.Transport.Nats.TransportTests
dotnet test src/Community.NServiceBus.Transport.Nats.AcceptanceTests
```

Environment variable: `NATS_CONNECTION_STRING` (default: `nats://localhost:4222` or `nats://nats:4222` in devcontainer)

## Lessons Learned

### NatsHeaders Becomes Read-Only After Publish
After calling `jetStream.PublishAsync()`, the headers object becomes immutable. When publishing to multiple subjects (polymorphic events), clone headers for each publish:

```csharp
foreach (var typeName in typeHierarchy)
{
    var publishHeaders = CloneHeaders(headers);  // Must clone!
    publishHeaders["Nats-Msg-Id"] = $"{messageId}-{typeName}";
    await jetStream.PublishAsync(subject, body, headers: publishHeaders);
}
```

### CI/CD Pattern

Follows Particular's conventions:
- Version tags without `v` prefix: `1.0.0`, `1.0.0-beta1`
- PowerShell as default shell in workflows
- Packages output to `nugets/` folder

### Debug Message Flow
```bash
# Watch messages in real-time
nats sub "nsb.>"

# View stream contents
nats stream view nsb-events --count 10

# Check consumer state
nats consumer info nsb-events myendpoint-events
```

## Important Files

| File | Purpose |
|------|---------|
| `MessageDispatcher.cs` | All message sending logic, polymorphic publishing |
| `MessagePump.cs` | Message receiving, concurrency control |
| `TopologyManager.cs` | Stream/consumer CRUD operations |
| `DelayedDeliveryProcessor.cs` | Background worker for delayed messages |
