namespace NServiceBus;

using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;
using NServiceBus.Transport;

sealed class NatsTransportInfrastructure : TransportInfrastructure
{
    readonly NatsConnectionManager connectionManager;
    readonly TopologyManager topologyManager;
    readonly DelayedDeliveryProcessor delayedDeliveryProcessor;
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

        delayedDeliveryProcessor = new DelayedDeliveryProcessor(jetStream, topologyManager);

        Receivers = receivers.ToDictionary(
            r => r.Id,
            r =>
            {
                var pump = new MessagePump(
                    r.Id,
                    ToTransportAddress(r.ReceiveAddress),
                    topologyManager,
                    jetStream,
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
        // Create the events stream for pub/sub
        await topologyManager.CreateEventsInfrastructure(cancellationToken);

        // Create delayed delivery infrastructure
        await topologyManager.CreateDelayedDeliveryInfrastructure(cancellationToken);

        // Start the delayed delivery processor
        await delayedDeliveryProcessor.Start(cancellationToken);

        // Create streams and consumers for each receiver
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
        await delayedDeliveryProcessor.Stop(cancellationToken);
        delayedDeliveryProcessor.Dispose();

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
