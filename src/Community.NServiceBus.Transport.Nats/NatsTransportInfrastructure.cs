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
        // Create events stream with Interest retention
        // This catch-all stream ensures event publishes always succeed
        // Messages are immediately deleted if no consumer is interested (no unbounded growth)
        await topologyManager.CreateEventsInfrastructure(cancellationToken);

        // Create streams and consumers for each receiver
        // Each endpoint stream captures unicast messages and schedule subjects
        // AllowMsgSchedules is enabled for native delayed delivery (NATS 2.12+, ADR-51)
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
