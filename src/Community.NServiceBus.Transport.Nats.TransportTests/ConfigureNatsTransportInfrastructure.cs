using NServiceBus;
using NServiceBus.Transport;
using NServiceBus.TransportTests;

class ConfigureNatsTransportInfrastructure : IConfigureTransportInfrastructure
{
    public TransportDefinition CreateTransportDefinition()
    {
        var connectionString = Environment.GetEnvironmentVariable("NatsTransport_ConnectionString")
            ?? Environment.GetEnvironmentVariable("NATS_URL")
            ?? "nats://localhost:4222";

        var transport = new NatsTransport(connectionString);

        return transport;
    }

    public async Task<TransportInfrastructure> Configure(
        TransportDefinition transportDefinition,
        HostSettings hostSettings,
        QueueAddress inputQueue,
        string errorQueueName,
        CancellationToken cancellationToken = default)
    {
        var mainReceiverSettings = new ReceiveSettings(
            "mainReceiver",
            inputQueue,
            true,
            false,
            errorQueueName);

        var transport = await transportDefinition.Initialize(
            hostSettings,
            [mainReceiverSettings],
            [errorQueueName],
            cancellationToken);

        queuesToCleanUp = [transport.ToTransportAddress(inputQueue), errorQueueName];
        return transport;
    }

    public Task Cleanup(CancellationToken cancellationToken = default)
    {
        // TODO: Clean up NATS streams/consumers
        return Task.CompletedTask;
    }

    string[]? queuesToCleanUp;
}
