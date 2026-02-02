# Configuration Guide

This guide covers NATS transport-specific configuration options.

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

**NKey authentication:**
```csharp
var transport = new NatsTransport("nats://localhost:4222")
{
    ConfigureNatsOptions = opts => opts with
    {
        AuthOpts = NatsAuthOpts.Default with
        {
            NKey = "UAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
            Seed = "SUAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
        }
    }
};
```

**JWT authentication with credentials file:**
```csharp
var transport = new NatsTransport("nats://localhost:4222")
{
    ConfigureNatsOptions = opts => opts with
    {
        AuthOpts = new NatsAuthOpts { CredsFile = "/path/to/user.creds" }
    }
};
```

### TLS Configuration

**Require TLS:**
```csharp
var transport = new NatsTransport("nats://localhost:4222")
{
    ConfigureNatsOptions = opts => opts with
    {
        TlsOpts = new NatsTlsOpts
        {
            Mode = TlsMode.Require
        }
    }
};
```

**Mutual TLS (mTLS) with client certificates:**
```csharp
var transport = new NatsTransport("nats://localhost:4222")
{
    ConfigureNatsOptions = opts => opts with
    {
        TlsOpts = new NatsTlsOpts
        {
            Mode = TlsMode.Require,
            CertFile = "/path/to/client-cert.pem",
            KeyFile = "/path/to/client-key.pem",
            CaFile = "/path/to/ca.pem"
        }
    }
};
```

### Advanced NATS Options

The `ConfigureNatsOptions` callback provides full access to the NATS client configuration. The transport configures sensible defaults, then your callback can customize any option:

```csharp
var transport = new NatsTransport("nats://localhost:4222")
{
    ConfigureNatsOptions = opts => opts with
    {
        // Override any NatsOpts property
        RequestTimeout = TimeSpan.FromSeconds(10),
        CommandTimeout = TimeSpan.FromSeconds(5),
        // Combine with auth/TLS as needed
        AuthOpts = new NatsAuthOpts { /* ... */ },
        TlsOpts = new NatsTlsOpts { /* ... */ }
    }
};
```

### Connection Resilience

The NATS.Net client handles reconnection automatically with exponential backoff. Configure the circuit breaker timeout:

```csharp
var transport = new NatsTransport("nats://localhost:4222")
{
    TimeToWaitBeforeTriggeringCircuitBreaker = TimeSpan.FromMinutes(2)  // Default
};
```

If the transport cannot reconnect within this time, it triggers a critical error.

## Logging

Integrate with Microsoft.Extensions.Logging:

```csharp
var transport = new NatsTransport("nats://localhost:4222")
{
    LoggerFactory = loggerFactory
};
```

## Health Checks

The transport provides two health check options for containerized deployments.

### ASP.NET Core Integration

For applications using ASP.NET Core, integrate with the health check middleware:

```csharp
var transport = new NatsTransport("nats://localhost:4222");
endpointConfiguration.UseTransport(transport);

// Register health check
services.AddNatsHealthCheck(transport, options =>
{
    options.Name = "nats";
    options.Tags = ["ready"];
});

// Map health endpoint
app.MapHealthChecks("/health");
```

### TCP Health Probe

For worker services or any container orchestration (Kubernetes, Docker, etc.), use the TCP health probe:

```csharp
var transport = new NatsTransport("nats://localhost:4222");
endpointConfiguration.UseTransport(transport);

// Register TCP health probe
services.AddNatsTcpHealthProbe(transport, options =>
{
    options.Port = 8081;
    options.BindAddress = "0.0.0.0";  // default
});
```

The probe accepts TCP connections and responds with `OK\n` when healthy or `FAIL\n` when unhealthy.

**Kubernetes:**
```yaml
livenessProbe:
  tcpSocket:
    port: 8081
```

**Docker/Docker Compose:**
```yaml
healthcheck:
  test: ["CMD", "nc", "-z", "localhost", "8081"]
  interval: 30s
```

### Health States

Both health checks report:
- **Healthy**: Connection is open
- **Degraded**: Connection is connecting or reconnecting
- **Unhealthy**: Connection is closed

## Transaction Modes

The transport supports `ReceiveOnly` (default) and `None` transaction modes. `SendsAtomicWithReceive` and `TransactionScope` are not supported because JetStream does not support distributed transactions.

## Requirements

- .NET 10.0 or later
- NServiceBus 10.0 or later
- NATS Server 2.12+ with JetStream enabled (required for native scheduled message delivery)

## Next Steps

- [Operations Guide](operations.md) - Deployment and NATS CLI commands
- [Architecture](architecture.md) - Technical implementation details
