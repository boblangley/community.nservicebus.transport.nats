namespace NServiceBus.Transport.Nats.Tests.HealthChecks;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using global::NATS.Client.Core;
using NUnit.Framework;

[TestFixture]
public class NatsHealthCheckTests
{
    [Test]
    public async Task Returns_unhealthy_when_transport_not_initialized()
    {
        var transport = new NatsTransport("nats://localhost:4222");
        var healthCheck = new NatsHealthCheck(transport, "test");

        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext());

        Assert.That(result.Status, Is.EqualTo(HealthStatus.Unhealthy));
        Assert.That(result.Description, Does.Contain("closed"));
    }

    [Test]
    public void GetConnectionState_returns_closed_before_initialization()
    {
        var transport = new NatsTransport("nats://localhost:4222");

        var state = transport.GetConnectionState();

        Assert.That(state, Is.EqualTo(NatsConnectionState.Closed));
    }

    [Test]
    public void Constructor_throws_when_transport_is_null()
    {
        Assert.Throws<ArgumentNullException>(() => new NatsHealthCheck(null!, "test"));
    }

    [Test]
    public async Task Uses_default_name_when_not_specified()
    {
        var transport = new NatsTransport("nats://localhost:4222");
        var healthCheck = new NatsHealthCheck(transport);

        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext());

        Assert.That(result.Description, Does.Contain("'nats'"));
    }

    [Test]
    public async Task Uses_custom_name_when_specified()
    {
        var transport = new NatsTransport("nats://localhost:4222");
        var healthCheck = new NatsHealthCheck(transport, "custom-name");

        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext());

        Assert.That(result.Description, Does.Contain("'custom-name'"));
    }
}
