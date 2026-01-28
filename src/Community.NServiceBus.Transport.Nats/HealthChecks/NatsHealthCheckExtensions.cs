namespace Microsoft.Extensions.DependencyInjection;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using NATS.Client.Core;
using NServiceBus;

/// <summary>
/// Extension methods for adding NATS health checks
/// </summary>
public static class NatsHealthCheckExtensions
{
    /// <summary>
    /// Adds a health check for a NATS connection
    /// </summary>
    /// <param name="builder">The health checks builder</param>
    /// <param name="connection">The NATS connection to monitor</param>
    /// <param name="name">The name of the health check (default: "nats")</param>
    /// <param name="failureStatus">The status to report when unhealthy</param>
    /// <param name="tags">Tags for filtering health checks</param>
    /// <returns>The health checks builder for chaining</returns>
    public static IHealthChecksBuilder AddNats(
        this IHealthChecksBuilder builder,
        NatsConnection connection,
        string name = "nats",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null)
    {
        return builder.Add(new HealthCheckRegistration(
            name,
            _ => new NatsHealthCheck(connection, name),
            failureStatus,
            tags));
    }
}
