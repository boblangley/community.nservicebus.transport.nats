namespace NServiceBus;

using System.Net;
using System.Net.Sockets;
using System.Text;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.Core;

/// <summary>
/// TCP health probe for NATS transport. Listens on a configured port and
/// responds to TCP connections with health status. Works with Kubernetes
/// TCP probes, Docker health checks, and exec-based probes.
/// </summary>
public sealed class NatsTcpHealthProbe : IHostedService, IDisposable
{
    readonly NatsTransport transport;
    readonly NatsTcpHealthProbeOptions options;
    readonly ILogger logger;
    readonly CancellationTokenSource cts = new();
    TcpListener? listener;
    Task? acceptTask;

    /// <summary>
    /// Creates a new TCP health probe
    /// </summary>
    /// <param name="transport">The NATS transport to monitor</param>
    /// <param name="options">Configuration options for the probe</param>
    /// <param name="loggerFactory">Optional logger factory</param>
    public NatsTcpHealthProbe(
        NatsTransport transport,
        NatsTcpHealthProbeOptions options,
        ILoggerFactory? loggerFactory = null)
    {
        this.transport = transport ?? throw new ArgumentNullException(nameof(transport));
        this.options = options ?? throw new ArgumentNullException(nameof(options));
        logger = loggerFactory?.CreateLogger<NatsTcpHealthProbe>() ?? NullLogger<NatsTcpHealthProbe>.Instance;
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        var bindAddress = IPAddress.Parse(options.BindAddress);
        listener = new TcpListener(bindAddress, options.Port);
        listener.Start();

        logger.LogInformation(
            "NATS TCP health probe listening on {Address}:{Port}",
            options.BindAddress,
            options.Port);

        acceptTask = AcceptConnectionsAsync(cts.Token);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Stopping NATS TCP health probe");

        await cts.CancelAsync();
        listener?.Stop();

        if (acceptTask != null)
        {
            try
            {
                await acceptTask.WaitAsync(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown
            }
        }
    }

    async Task AcceptConnectionsAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var client = await listener!.AcceptTcpClientAsync(cancellationToken);
                _ = HandleClientAsync(client, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (SocketException ex) when (ex.SocketErrorCode == SocketError.OperationAborted)
            {
                // Listener was stopped
                break;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Error accepting TCP health probe connection");
            }
        }
    }

    async Task HandleClientAsync(TcpClient client, CancellationToken cancellationToken)
    {
        try
        {
            using (client)
            {
                var state = transport.GetConnectionState();
                var isHealthy = state == NatsConnectionState.Open;

                var response = isHealthy ? "OK\n" : "FAIL\n";
                var responseBytes = Encoding.UTF8.GetBytes(response);

                await using var stream = client.GetStream();
                await stream.WriteAsync(responseBytes, cancellationToken);
                await stream.FlushAsync(cancellationToken);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogDebug(ex, "Error handling TCP health probe client");
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        cts.Dispose();
        listener?.Stop();
    }
}
