namespace NServiceBus;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using NATS.Client.Core;

/// <summary>
/// Health check for NATS connection status
/// </summary>
public sealed class NatsHealthCheck : IHealthCheck
{
    readonly NatsConnection connection;
    readonly string name;

    /// <summary>
    /// Creates a new NATS health check
    /// </summary>
    /// <param name="connection">The NATS connection to check</param>
    /// <param name="name">Optional name for the health check</param>
    public NatsHealthCheck(NatsConnection connection, string? name = null)
    {
        this.connection = connection ?? throw new ArgumentNullException(nameof(connection));
        this.name = name ?? "nats";
    }

    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        var state = connection.ConnectionState;

        return Task.FromResult(state switch
        {
            NatsConnectionState.Open => HealthCheckResult.Healthy(
                $"NATS connection '{name}' is open. Server: {connection.ServerInfo?.Name ?? "unknown"}"),

            NatsConnectionState.Connecting => HealthCheckResult.Degraded(
                $"NATS connection '{name}' is connecting..."),

            NatsConnectionState.Reconnecting => HealthCheckResult.Degraded(
                $"NATS connection '{name}' is reconnecting..."),

            NatsConnectionState.Closed => HealthCheckResult.Unhealthy(
                $"NATS connection '{name}' is closed"),

            _ => HealthCheckResult.Unhealthy(
                $"NATS connection '{name}' is in unknown state: {state}")
        });
    }
}
