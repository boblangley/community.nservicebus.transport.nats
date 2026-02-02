namespace NServiceBus.Transport.Nats.Tests.HealthChecks;

using System.Net.Sockets;
using System.Text;
using NUnit.Framework;

[TestFixture]
public class NatsTcpHealthProbeTests
{
    [Test]
    public async Task Starts_and_accepts_connections()
    {
        var transport = new NatsTransport("nats://localhost:4222");
        var options = new NatsTcpHealthProbeOptions { Port = 18081 };
        var probe = new NatsTcpHealthProbe(transport, options);

        await probe.StartAsync(CancellationToken.None);

        try
        {
            using var client = new TcpClient();
            await client.ConnectAsync("127.0.0.1", 18081);

            Assert.That(client.Connected, Is.True);
        }
        finally
        {
            await probe.StopAsync(CancellationToken.None);
            probe.Dispose();
        }
    }

    [Test]
    public async Task Returns_fail_when_transport_not_initialized()
    {
        var transport = new NatsTransport("nats://localhost:4222");
        var options = new NatsTcpHealthProbeOptions { Port = 18082 };
        var probe = new NatsTcpHealthProbe(transport, options);

        await probe.StartAsync(CancellationToken.None);

        try
        {
            using var client = new TcpClient();
            await client.ConnectAsync("127.0.0.1", 18082);

            await using var stream = client.GetStream();
            var buffer = new byte[16];
            var bytesRead = await stream.ReadAsync(buffer);
            var response = Encoding.UTF8.GetString(buffer, 0, bytesRead);

            Assert.That(response, Is.EqualTo("FAIL\n"));
        }
        finally
        {
            await probe.StopAsync(CancellationToken.None);
            probe.Dispose();
        }
    }

    [Test]
    public async Task Stops_cleanly()
    {
        var transport = new NatsTransport("nats://localhost:4222");
        var options = new NatsTcpHealthProbeOptions { Port = 18083 };
        var probe = new NatsTcpHealthProbe(transport, options);

        await probe.StartAsync(CancellationToken.None);
        await probe.StopAsync(CancellationToken.None);
        probe.Dispose();

        // Verify port is no longer in use
        using var client = new TcpClient();
        Assert.ThrowsAsync<SocketException>(async () =>
            await client.ConnectAsync("127.0.0.1", 18083));
    }

    [Test]
    public void Constructor_throws_when_transport_is_null()
    {
        var options = new NatsTcpHealthProbeOptions { Port = 18084 };
        Assert.Throws<ArgumentNullException>(() => new NatsTcpHealthProbe(null!, options));
    }

    [Test]
    public void Constructor_throws_when_options_is_null()
    {
        var transport = new NatsTransport("nats://localhost:4222");
        Assert.Throws<ArgumentNullException>(() => new NatsTcpHealthProbe(transport, null!));
    }

    [Test]
    public void Options_default_bind_address_is_all_interfaces()
    {
        var options = new NatsTcpHealthProbeOptions();

        Assert.That(options.BindAddress, Is.EqualTo("0.0.0.0"));
    }
}
