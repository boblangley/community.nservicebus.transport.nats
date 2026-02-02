namespace NServiceBus;

using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;
using NServiceBus.Transport;

sealed class NatsTransportInfrastructure : TransportInfrastructure
{
    readonly NatsConnectionManager connectionManager;
    readonly TopologyManager topologyManager;
    readonly string streamPrefix;
    readonly List<MessagePump> messagePumps = [];

    public NatsTransportInfrastructure(
        NatsConnectionManager connectionManager,
        TopologyManager topologyManager,
        NatsJSContext jetStream,
        ReceiveSettings[] receivers,
        string streamPrefix,
        Action<string, Exception, CancellationToken> criticalErrorAction,
        TimeSpan circuitBreakerTimeout,
        ILoggerFactory? loggerFactory = null)
    {
        this.connectionManager = connectionManager;
        this.topologyManager = topologyManager;
        this.streamPrefix = streamPrefix;

        Receivers = receivers.ToDictionary(
            r => r.Id,
            r =>
            {
                var pump = new MessagePump(
                    r.Id,
                    ToTransportAddress(r.ReceiveAddress),
                    topologyManager,
                    criticalErrorAction,
                    circuitBreakerTimeout,
                    loggerFactory);
                messagePumps.Add(pump);
                return (IMessageReceiver)pump;
            });

        Dispatcher = new MessageDispatcher(jetStream, topologyManager);
    }

    public async Task SetupInfrastructure(
        ReceiveSettings[] receivers,
        string[] sendingAddresses,
        CancellationToken cancellationToken = default)
    {
        // Create central streams first - endpoint streams source from them
        // Events stream: captures all published events
        await topologyManager.CreateEventsStream(cancellationToken);
        // Delayed stream: handles native NATS scheduling with ready subject delivery
        await topologyManager.CreateDelayedStream(cancellationToken);

        // Create streams and consumers for each receiver
        // Each endpoint stream:
        // - Captures unicast messages: {prefix}.endpoint.{endpoint}
        // - Sources from events stream for subscribed event types
        // - Sources from delayed stream for ready messages: {prefix}.ready.{endpoint}
        foreach (var receiver in receivers)
        {
            var address = ToTransportAddress(receiver.ReceiveAddress);
            await topologyManager.CreateEndpointInfrastructure(address, cancellationToken);
        }

        // Create streams for error queues
        foreach (var receiver in receivers)
        {
            if (!string.IsNullOrEmpty(receiver.ErrorQueue))
            {
                await topologyManager.CreateEndpointInfrastructure(receiver.ErrorQueue, cancellationToken);
            }
        }

        // Create streams for sending addresses
        foreach (var address in sendingAddresses)
        {
            await topologyManager.CreateEndpointInfrastructure(address, cancellationToken);
        }
    }

    public override async Task Shutdown(CancellationToken cancellationToken = default)
    {
        foreach (var pump in messagePumps)
        {
            pump.Dispose();
        }

        await connectionManager.DisposeAsync();
    }

    public override string ToTransportAddress(QueueAddress address)
    {
        var baseAddress = address.BaseAddress;

        if (address.Discriminator != null)
        {
            baseAddress += "-" + address.Discriminator;
        }

        if (address.Qualifier != null)
        {
            baseAddress = address.Qualifier + "-" + baseAddress;
        }

        return baseAddress;
    }
}
