namespace Microsoft.Extensions.DependencyInjection;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using NServiceBus;

/// <summary>
/// Extension methods for adding NATS health checks to the service collection
/// </summary>
public static class NatsHealthCheckExtensions
{
    /// <summary>
    /// Adds a NATS health check that integrates with ASP.NET Core health check infrastructure.
    /// Use with <c>app.MapHealthChecks("/health")</c> to expose an HTTP health endpoint.
    /// </summary>
    /// <param name="services">The service collection</param>
    /// <param name="transport">The NATS transport to monitor</param>
    /// <param name="configure">Optional configuration for the health check</param>
    /// <returns>The service collection for chaining</returns>
    /// <example>
    /// <code>
    /// services.AddNatsHealthCheck(transport);
    /// services.AddNatsHealthCheck(transport, options =>
    /// {
    ///     options.Name = "nats-connection";
    ///     options.Tags = ["ready", "live"];
    /// });
    /// </code>
    /// </example>
    public static IServiceCollection AddNatsHealthCheck(
        this IServiceCollection services,
        NatsTransport transport,
        Action<NatsHealthCheckOptions>? configure = null)
    {
        var options = new NatsHealthCheckOptions();
        configure?.Invoke(options);

        services.AddHealthChecks()
            .Add(new HealthCheckRegistration(
                options.Name,
                _ => new NatsHealthCheck(transport, options.Name),
                options.FailureStatus,
                options.Tags));

        return services;
    }

    /// <summary>
    /// Adds a TCP health probe that listens on a specified port.
    /// Works with Kubernetes TCP probes, Docker health checks, and exec-based probes.
    /// </summary>
    /// <param name="services">The service collection</param>
    /// <param name="transport">The NATS transport to monitor</param>
    /// <param name="configure">Configuration for the TCP probe (port is required)</param>
    /// <returns>The service collection for chaining</returns>
    /// <example>
    /// <code>
    /// services.AddNatsTcpHealthProbe(transport, options =>
    /// {
    ///     options.Port = 8081;
    ///     options.BindAddress = "0.0.0.0";  // default
    /// });
    /// </code>
    /// </example>
    /// <remarks>
    /// The probe responds to TCP connections with "OK\n" when healthy or "FAIL\n" when unhealthy.
    ///
    /// Kubernetes example:
    /// <code>
    /// livenessProbe:
    ///   tcpSocket:
    ///     port: 8081
    /// </code>
    ///
    /// Docker example:
    /// <code>
    /// HEALTHCHECK CMD nc -z localhost 8081 || exit 1
    /// </code>
    /// </remarks>
    public static IServiceCollection AddNatsTcpHealthProbe(
        this IServiceCollection services,
        NatsTransport transport,
        Action<NatsTcpHealthProbeOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var options = new NatsTcpHealthProbeOptions();
        configure(options);

        if (options.Port <= 0)
        {
            throw new ArgumentException("Port must be specified and greater than 0", nameof(configure));
        }

        services.AddHostedService(sp =>
        {
            var loggerFactory = sp.GetService<ILoggerFactory>();
            return new NatsTcpHealthProbe(transport, options, loggerFactory);
        });

        return services;
    }
}
