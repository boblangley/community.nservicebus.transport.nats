namespace NServiceBus;

using Microsoft.Extensions.Diagnostics.HealthChecks;

/// <summary>
/// Options for configuring NATS ASP.NET Core health check registration
/// </summary>
public sealed class NatsHealthCheckOptions
{
    /// <summary>
    /// The name of the health check. Default: "nats"
    /// </summary>
    public string Name { get; set; } = "nats";

    /// <summary>
    /// Tags for filtering health checks. Default: empty
    /// </summary>
    public IEnumerable<string> Tags { get; set; } = [];

    /// <summary>
    /// The status to report when the health check fails. Default: Unhealthy
    /// </summary>
    public HealthStatus? FailureStatus { get; set; }
}
