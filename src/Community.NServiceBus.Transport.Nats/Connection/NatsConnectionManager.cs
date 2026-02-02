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
        string connectionName,
        ILoggerFactory? loggerFactory = null,
        Action<string, Exception, CancellationToken>? criticalErrorAction = null,
        Func<NatsOpts, NatsOpts>? configureOptions = null)
    {
        this.criticalErrorAction = criticalErrorAction;
        logger = loggerFactory?.CreateLogger<NatsConnectionManager>() ?? NullLogger<NatsConnectionManager>.Instance;

        var opts = NatsOpts.Default with
        {
            Url = connectionString,
            Name = connectionName
        };

        // Apply user customizations if provided (authentication, TLS, reconnection settings, etc.)
        if (configureOptions != null)
        {
            opts = configureOptions(opts);
        }

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

        EnsureMinimumServerVersion();
    }

    void EnsureMinimumServerVersion()
    {
        var serverInfo = connection.ServerInfo;
        if (serverInfo == null)
        {
            throw new InvalidOperationException(
                "Unable to determine NATS server version. The NServiceBus NATS transport requires NATS server 2.12 or later for native scheduled message delivery support.");
        }

        var versionString = serverInfo.Version;
        if (string.IsNullOrEmpty(versionString))
        {
            throw new InvalidOperationException(
                "Unable to determine NATS server version. The NServiceBus NATS transport requires NATS server 2.12 or later for native scheduled message delivery support.");
        }

        // Parse version string (e.g., "2.12.0", "2.12.0-beta.1")
        var versionPart = versionString.Split('-')[0]; // Remove pre-release suffix
        var parts = versionPart.Split('.');

        if (parts.Length < 2 ||
            !int.TryParse(parts[0], out var major) ||
            !int.TryParse(parts[1], out var minor))
        {
            throw new InvalidOperationException(
                $"Unable to parse NATS server version '{versionString}'. The NServiceBus NATS transport requires NATS server 2.12 or later for native scheduled message delivery support.");
        }

        const int requiredMajor = 2;
        const int requiredMinor = 12;

        if (major < requiredMajor || (major == requiredMajor && minor < requiredMinor))
        {
            throw new InvalidOperationException(
                $"NATS server version {versionString} is not supported. The NServiceBus NATS transport requires NATS server {requiredMajor}.{requiredMinor} or later for native scheduled message delivery support. " +
                $"Please upgrade your NATS server to version {requiredMajor}.{requiredMinor} or later.");
        }

        logger.LogInformation(
            "NATS server version {Version} meets minimum requirement ({Required}+). Server: {ServerName}, Cluster: {ClusterName}",
            versionString,
            $"{requiredMajor}.{requiredMinor}",
            serverInfo.Name ?? "unknown",
            serverInfo.Cluster ?? "unknown");
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
