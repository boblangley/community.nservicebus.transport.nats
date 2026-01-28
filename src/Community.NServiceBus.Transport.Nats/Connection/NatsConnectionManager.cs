namespace NServiceBus;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.Core;
using NATS.Client.JetStream;

sealed class NatsConnectionManager : IAsyncDisposable
{
    readonly NatsConnection connection;
    readonly NatsJSContext jetStream;
    readonly ILogger logger;
    readonly Action<string, Exception, CancellationToken>? criticalErrorAction;

    public NatsConnectionManager(
        string connectionString,
        NatsConnectionSettings? settings = null,
        ILoggerFactory? loggerFactory = null,
        Action<string, Exception, CancellationToken>? criticalErrorAction = null)
    {
        settings ??= new NatsConnectionSettings();
        this.criticalErrorAction = criticalErrorAction;
        logger = loggerFactory?.CreateLogger<NatsConnectionManager>() ?? NullLogger<NatsConnectionManager>.Instance;

        var opts = NatsOpts.Default with
        {
            Url = connectionString,
            Name = settings.ClientName,
            ConnectTimeout = settings.ConnectTimeout,
            MaxReconnectRetry = settings.MaxReconnectRetry,
            ReconnectWaitMin = settings.ReconnectWaitMin,
            ReconnectWaitMax = settings.ReconnectWaitMax,
            ReconnectJitter = settings.ReconnectJitter,
            PingInterval = settings.PingInterval,
            MaxPingOut = settings.MaxPingOut
        };

        connection = new NatsConnection(opts);
        jetStream = new NatsJSContext(connection);

        // Subscribe to connection events
        connection.ConnectionDisconnected += OnConnectionDisconnected;
        connection.ConnectionOpened += OnConnectionOpened;
        connection.ReconnectFailed += OnReconnectFailed;
    }

    public NatsConnection Connection => connection;

    public NatsJSContext JetStream => jetStream;

    public bool IsConnected => connection.ConnectionState == NatsConnectionState.Open;

    public NatsConnectionState ConnectionState => connection.ConnectionState;

    public async ValueTask ConnectAsync(CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Connecting to NATS server...");
        await connection.ConnectAsync().AsTask().WaitAsync(cancellationToken);
        logger.LogInformation("Connected to NATS server at {ServerInfo}", connection.ServerInfo?.Name ?? "unknown");
    }

    ValueTask OnConnectionDisconnected(object? sender, NatsEventArgs args)
    {
        logger.LogWarning("Disconnected from NATS server. Attempting to reconnect...");
        return ValueTask.CompletedTask;
    }

    ValueTask OnConnectionOpened(object? sender, NatsEventArgs args)
    {
        logger.LogInformation("Connection to NATS server established/restored");
        return ValueTask.CompletedTask;
    }

    ValueTask OnReconnectFailed(object? sender, NatsEventArgs args)
    {
        logger.LogError("Failed to reconnect to NATS server");

        // Invoke critical error if max retries exhausted
        criticalErrorAction?.Invoke(
            "NATS connection failed and could not be restored",
            new InvalidOperationException("NATS reconnection failed"),
            CancellationToken.None);

        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        connection.ConnectionDisconnected -= OnConnectionDisconnected;
        connection.ConnectionOpened -= OnConnectionOpened;
        connection.ReconnectFailed -= OnReconnectFailed;
        await connection.DisposeAsync();
    }
}

/// <summary>
/// Settings for NATS connection behavior
/// </summary>
public sealed class NatsConnectionSettings
{
    /// <summary>
    /// Client name for identification in NATS server
    /// </summary>
    public string ClientName { get; set; } = "NServiceBus";

    /// <summary>
    /// Connection establishment timeout (default: 10 seconds)
    /// </summary>
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Maximum reconnection attempts. -1 for unlimited (default: -1)
    /// </summary>
    public int MaxReconnectRetry { get; set; } = -1;

    /// <summary>
    /// Minimum backoff delay between reconnection attempts (default: 2 seconds)
    /// </summary>
    public TimeSpan ReconnectWaitMin { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Maximum backoff delay between reconnection attempts (default: 30 seconds)
    /// </summary>
    public TimeSpan ReconnectWaitMax { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Random jitter added to backoff delay (default: 100ms)
    /// </summary>
    public TimeSpan ReconnectJitter { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Interval between server pings (default: 2 minutes)
    /// </summary>
    public TimeSpan PingInterval { get; set; } = TimeSpan.FromMinutes(2);

    /// <summary>
    /// Maximum unanswered pings before triggering reconnect (default: 2)
    /// </summary>
    public int MaxPingOut { get; set; } = 2;
}
