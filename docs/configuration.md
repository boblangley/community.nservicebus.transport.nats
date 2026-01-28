# Configuration Guide

This guide covers how to configure and use the NATS JetStream transport for NServiceBus.

## Installation

```bash
dotnet add package Community.NServiceBus.Transport.Nats
```

## Basic Setup

```csharp
var endpointConfiguration = new EndpointConfiguration("MyEndpoint");

var transport = new NatsTransport("nats://localhost:4222");
endpointConfiguration.UseTransport(transport);

var endpoint = await Endpoint.Start(endpointConfiguration);
```

## Transport Options

### Stream Prefix

All JetStream streams are prefixed to avoid conflicts with other applications:

```csharp
var transport = new NatsTransport("nats://localhost:4222")
{
    StreamPrefix = "myapp"  // Default: "nsb"
};
```

This creates streams named `myapp-{endpoint}`, `myapp-events`, and `myapp-delayed`.

### Acknowledgment Timeout

How long JetStream waits for message acknowledgment before redelivery:

```csharp
var transport = new NatsTransport("nats://localhost:4222")
{
    AckWait = TimeSpan.FromSeconds(60)  // Default: 30 seconds
};
```

Increase this for long-running message handlers.

### Message Deduplication

JetStream provides server-side message deduplication:

```csharp
var transport = new NatsTransport("nats://localhost:4222")
{
    EnableMessageDeduplication = true,       // Default: true
    DeduplicationWindow = TimeSpan.FromMinutes(5)  // Default: 2 minutes
};
```

## Connection Configuration

### Connection String

The connection string supports multiple servers for high availability:

```csharp
var transport = new NatsTransport("nats://server1:4222,nats://server2:4222,nats://server3:4222");
```

### Authentication

**Token authentication:**
```csharp
var transport = new NatsTransport("nats://mytoken@localhost:4222");
```

**Username/password:**
```csharp
var transport = new NatsTransport("nats://user:password@localhost:4222");
```

### Connection Resilience

The NATS.Net client handles reconnection automatically with exponential backoff. Configure the circuit breaker timeout:

```csharp
var transport = new NatsTransport("nats://localhost:4222")
{
    TimeToWaitBeforeTriggeringCircuitBreaker = TimeSpan.FromMinutes(2)  // Default
};
```

If the transport cannot connect within this time, it triggers a critical error.

## Logging

Integrate with Microsoft.Extensions.Logging:

```csharp
var transport = new NatsTransport("nats://localhost:4222")
{
    LoggerFactory = loggerFactory
};
```

## Delayed Delivery

Send messages for future delivery using standard NServiceBus APIs:

```csharp
// Delay by duration
var options = new SendOptions();
options.DelayDeliveryWith(TimeSpan.FromMinutes(30));
await context.Send(new ProcessOrder(), options);

// Deliver at specific time
var options = new SendOptions();
options.DoNotDeliverBefore(DateTimeOffset.UtcNow.AddHours(2));
await context.Send(new SendReminder(), options);
```

Delayed messages are stored in a dedicated stream and forwarded when their delivery time arrives.

## Time-To-Be-Received (TTBR)

Set message expiration to discard stale messages:

```csharp
[TimeToBeReceived("00:05:00")]  // Expires after 5 minutes
public class StockQuote : IMessage
{
    public string Symbol { get; set; }
    public decimal Price { get; set; }
}
```

> **Note:** TTBR and delayed delivery cannot be combined on the same message.

## Publishing Events

### Basic Publishing

```csharp
public class OrderPlacedHandler : IHandleMessages<PlaceOrder>
{
    public async Task Handle(PlaceOrder message, IMessageHandlerContext context)
    {
        // Process order...

        await context.Publish(new OrderPlaced { OrderId = message.OrderId });
    }
}
```

### Polymorphic Subscriptions

The transport supports subscribing to base types and interfaces:

```csharp
// Event hierarchy
public interface IOrderEvent { Guid OrderId { get; } }
public class OrderPlaced : IOrderEvent { public Guid OrderId { get; set; } }
public class OrderShipped : IOrderEvent { public Guid OrderId { get; set; } }

// Subscribe to all order events
public class OrderAuditHandler : IHandleMessages<IOrderEvent>
{
    public Task Handle(IOrderEvent message, IMessageHandlerContext context)
    {
        // Handles OrderPlaced, OrderShipped, and any future IOrderEvent
        Console.WriteLine($"Order event: {message.OrderId}");
        return Task.CompletedTask;
    }
}
```

### Duplicate Handling with Outbox

When subscribing to multiple types in the same hierarchy (e.g., both `OrderPlaced` and `IOrderEvent`), enable the Outbox for idempotent processing:

```csharp
var endpointConfiguration = new EndpointConfiguration("MyEndpoint");

// Enable Outbox
endpointConfiguration.EnableOutbox();

// Configure persistence for Outbox storage
var persistence = endpointConfiguration.UsePersistence<SqlPersistence>();
persistence.ConnectionBuilder(() => new SqlConnection(connectionString));
```

The Outbox ensures each message is processed exactly once based on its `NServiceBus.MessageId`.

## Transaction Modes

The transport supports two transaction modes:

| Mode | Description |
|------|-------------|
| `None` | No transaction guarantees. Messages may be lost on failure. |
| `ReceiveOnly` | Messages are acknowledged after successful processing. Default and recommended. |

```csharp
var transport = new NatsTransport("nats://localhost:4222");
transport.TransportTransactionMode = TransportTransactionMode.ReceiveOnly;
```

> **Note:** `SendsAtomicWithReceive` and `TransactionScope` are not supported. JetStream does not support distributed transactions.

## Health Checks

Add NATS health checks to ASP.NET Core:

```csharp
// In Program.cs
builder.Services.AddHealthChecks()
    .AddCheck<NatsHealthCheck>("nats");

// Register the health check with the NATS connection
builder.Services.AddSingleton<NatsHealthCheck>(sp =>
    new NatsHealthCheck(natsConnection));
```

Or use the extension method if you have access to the connection:

```csharp
builder.Services.AddHealthChecks()
    .AddNats(natsConnection, name: "nats", tags: new[] { "ready" });
```

## Concurrency

Configure message processing concurrency:

```csharp
var endpointConfiguration = new EndpointConfiguration("MyEndpoint");
endpointConfiguration.LimitMessageProcessingConcurrencyTo(16);  // Default: number of logical processors
```

## Requirements

- .NET 10.0 or later
- NServiceBus 10.0 or later
- NATS Server 2.10+ with JetStream enabled

## Next Steps

- [Operations Guide](operations.md) - Deployment and infrastructure setup
- [Architecture](architecture.md) - Technical implementation details
