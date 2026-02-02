namespace NServiceBus;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using NATS.Client.Core;

/// <summary>
/// Health check for NATS transport connection status.
/// Integrates with ASP.NET Core health check infrastructure.
/// </summary>
public sealed class NatsHealthCheck : IHealthCheck
{
    readonly NatsTransport transport;
    readonly string name;

    /// <summary>
    /// Creates a new NATS health check
    /// </summary>
    /// <param name="transport">The NATS transport to check</param>
    /// <param name="name">Optional name for the health check</param>
    public NatsHealthCheck(NatsTransport transport, string? name = null)
    {
        this.transport = transport ?? throw new ArgumentNullException(nameof(transport));
        this.name = name ?? "nats";
    }

    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        var state = transport.GetConnectionState();

        return Task.FromResult(state switch
        {
            NatsConnectionState.Open => HealthCheckResult.Healthy(
                $"NATS connection '{name}' is open. Server: {transport.ServerName ?? "unknown"}"),

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
