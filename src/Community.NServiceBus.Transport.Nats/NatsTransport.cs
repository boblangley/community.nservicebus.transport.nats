namespace NServiceBus;

using Microsoft.Extensions.Logging;
using NServiceBus.Transport;

public sealed class NatsTransport : TransportDefinition
{
    public NatsTransport(string connectionString)
        : base(TransportTransactionMode.ReceiveOnly,
               supportsDelayedDelivery: true,
               supportsPublishSubscribe: true,
               supportsTTBR: true)
    {
        ConnectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
    }

    public string ConnectionString { get; }

    public string StreamPrefix { get; set; } = "nsb";

    public int MaxDeliveryAttempts { get; set; } = 5;

    public TimeSpan AckWait { get; set; } = TimeSpan.FromSeconds(30);

    public int PrefetchCount { get; set; } = 100;

    public bool EnableMessageDeduplication { get; set; } = true;

    public TimeSpan DeduplicationWindow { get; set; } = TimeSpan.FromMinutes(2);

    public TimeSpan TimeToWaitBeforeTriggeringCircuitBreaker { get; set; } = TimeSpan.FromMinutes(2);

    /// <summary>
    /// Connection settings for NATS client
    /// </summary>
    public NatsConnectionSettings ConnectionSettings { get; set; } = new();

    /// <summary>
    /// Optional logger factory for transport logging
    /// </summary>
    public ILoggerFactory? LoggerFactory { get; set; }

    public override async Task<TransportInfrastructure> Initialize(
        HostSettings hostSettings,
        ReceiveSettings[] receivers,
        string[] sendingAddresses,
        CancellationToken cancellationToken = default)
    {
        var connectionManager = new NatsConnectionManager(
            ConnectionString,
            ConnectionSettings,
            LoggerFactory,
            hostSettings.CriticalErrorAction);
        await connectionManager.ConnectAsync(cancellationToken);

        var jetStream = connectionManager.JetStream;
        var topologyManager = new TopologyManager(jetStream, StreamPrefix);

        var infrastructure = new NatsTransportInfrastructure(
            connectionManager,
            topologyManager,
            jetStream,
            receivers,
            StreamPrefix,
            hostSettings.CriticalErrorAction,
            TimeToWaitBeforeTriggeringCircuitBreaker,
            LoggerFactory);

        if (hostSettings.SetupInfrastructure)
        {
            await infrastructure.SetupInfrastructure(receivers, sendingAddresses, cancellationToken);
        }

        WriteStartupDiagnostics(hostSettings, connectionManager);

        return infrastructure;
    }

    public override IReadOnlyCollection<TransportTransactionMode> GetSupportedTransactionModes() =>
        [TransportTransactionMode.None, TransportTransactionMode.ReceiveOnly];

    void WriteStartupDiagnostics(HostSettings hostSettings, NatsConnectionManager connectionManager)
    {
        var serverInfo = connectionManager.Connection.ServerInfo;

        hostSettings.StartupDiagnostic.Add("NServiceBus.Transport.Nats", new
        {
            StreamPrefix,
            ServerVersion = serverInfo?.Version ?? "unknown",
            ServerName = serverInfo?.Name ?? "unknown",
            Cluster = serverInfo?.Cluster ?? "none",
            JetStreamEnabled = true, // Required for this transport
            MaxDeliveryAttempts,
            AckWait = AckWait.ToString(),
            PrefetchCount,
            EnableMessageDeduplication,
            DeduplicationWindow = DeduplicationWindow.ToString(),
            TimeToWaitBeforeTriggeringCircuitBreaker = TimeToWaitBeforeTriggeringCircuitBreaker.ToString(),
            DelayedDelivery = "Native (NATS 2.12+ scheduled messages)"
        });
    }
}
