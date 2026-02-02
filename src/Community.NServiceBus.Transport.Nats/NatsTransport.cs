namespace NServiceBus;

using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NServiceBus.Transport;

public sealed class NatsTransport : TransportDefinition
{
    NatsConnectionManager? connectionManager;

    public NatsTransport(string connectionString)
        : base(TransportTransactionMode.ReceiveOnly,
               supportsDelayedDelivery: true,
               supportsPublishSubscribe: true,
               supportsTTBR: true)
    {
        ConnectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
    }

    public string ConnectionString { get; }

    /// <summary>
    /// Prefix for all JetStream stream names. Default: "nsb"
    /// </summary>
    public string StreamPrefix { get; set; } = "nsb";

    /// <summary>
    /// How long JetStream waits for message acknowledgment before redelivery.
    /// Increase this for long-running message handlers. Default: 30 seconds.
    /// </summary>
    public TimeSpan AckWait { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How long to wait before triggering a critical error when the transport
    /// cannot connect or reconnect to NATS. Default: 2 minutes.
    /// </summary>
    public TimeSpan TimeToWaitBeforeTriggeringCircuitBreaker { get; set; } = TimeSpan.FromMinutes(2);

    /// <summary>
    /// Optional logger factory for transport logging
    /// </summary>
    public ILoggerFactory? LoggerFactory { get; set; }

    /// <summary>
    /// Name used to identify this connection in NATS server monitoring.
    /// Defaults to the endpoint name if not specified.
    /// </summary>
    public string? ConnectionName { get; set; }

    /// <summary>
    /// Optional callback to customize the NATS client options.
    /// Use this for authentication, TLS, reconnection settings, or any other NatsOpts configuration.
    /// The callback receives the pre-configured options and should return the modified options.
    /// </summary>
    /// <example>
    /// <code>
    /// // NKey authentication
    /// transport.ConfigureNatsOptions = opts => opts with
    /// {
    ///     AuthOpts = NatsAuthOpts.Default with
    ///     {
    ///         NKey = "SUACSSL3...",
    ///         Seed = "SUACSSL3..."
    ///     }
    /// };
    ///
    /// // JWT authentication with credentials file
    /// transport.ConfigureNatsOptions = opts => opts with
    /// {
    ///     AuthOpts = new NatsAuthOpts { CredsFile = "/path/to/user.creds" }
    /// };
    ///
    /// // TLS configuration
    /// transport.ConfigureNatsOptions = opts => opts with
    /// {
    ///     TlsOpts = new NatsTlsOpts
    ///     {
    ///         Mode = TlsMode.Require,
    ///         CertFile = "/path/to/cert.pem",
    ///         KeyFile = "/path/to/key.pem"
    ///     }
    /// };
    ///
    /// // Custom reconnection settings
    /// transport.ConfigureNatsOptions = opts => opts with
    /// {
    ///     MaxReconnectRetry = 10,
    ///     ReconnectWaitMin = TimeSpan.FromSeconds(1),
    ///     ReconnectWaitMax = TimeSpan.FromSeconds(30)
    /// };
    /// </code>
    /// </example>
    public Func<NatsOpts, NatsOpts>? ConfigureNatsOptions { get; set; }

    /// <summary>
    /// Gets the current NATS connection state. Returns <see cref="NatsConnectionState.Closed"/>
    /// if the transport has not been initialized yet.
    /// </summary>
    /// <remarks>
    /// This method is primarily intended for health checks to monitor connection status.
    /// </remarks>
    public NatsConnectionState GetConnectionState() =>
        connectionManager?.ConnectionState ?? NatsConnectionState.Closed;

    /// <summary>
    /// Gets the NATS server name. Returns null if the transport has not been initialized
    /// or if the connection is not established.
    /// </summary>
    internal string? ServerName => connectionManager?.Connection.ServerInfo?.Name;

    public override async Task<TransportInfrastructure> Initialize(
        HostSettings hostSettings,
        ReceiveSettings[] receivers,
        string[] sendingAddresses,
        CancellationToken cancellationToken = default)
    {
        connectionManager = new NatsConnectionManager(
            ConnectionString,
            ConnectionName ?? hostSettings.Name,
            LoggerFactory,
            hostSettings.CriticalErrorAction,
            ConfigureNatsOptions);
        await connectionManager.ConnectAsync(cancellationToken);

        var jetStream = connectionManager.JetStream;
        var topologyManager = new TopologyManager(jetStream, StreamPrefix, AckWait);

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
            AckWait = AckWait.ToString(),
            TimeToWaitBeforeTriggeringCircuitBreaker = TimeToWaitBeforeTriggeringCircuitBreaker.ToString(),
            DelayedDelivery = "Native (NATS 2.12+ scheduled messages)"
        });
    }
}
