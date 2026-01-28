using NServiceBus;
using NServiceBus.AcceptanceTesting.Support;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NUnit.Framework;

class ConfigureEndpointNatsTransport : IConfigureEndpointTestExecution
{
    NatsTransport? transport;
    string? streamPrefix;
    NatsConnection? cleanupConnection;

    public Task Configure(string endpointName, EndpointConfiguration configuration, RunSettings settings, PublisherMetadata publisherMetadata)
    {
        var connectionString = Environment.GetEnvironmentVariable("NATS_CONNECTION_STRING") ?? "nats://nats:4222";

        // Use the test name to generate a consistent prefix across all endpoints in the same test
        // This ensures sender and receiver use the same stream prefix
        var testName = TestContext.CurrentContext.Test.FullName;
        var hash = (uint)testName.GetHashCode();
        streamPrefix = $"t{hash:x8}";

        transport = new NatsTransport(connectionString)
        {
            StreamPrefix = streamPrefix
        };

        configuration.UseTransport(transport);

        return Task.CompletedTask;
    }

    public async Task Cleanup()
    {
        if (transport == null || streamPrefix == null)
        {
            return;
        }

        await CleanupStreams();
    }

    async Task CleanupStreams()
    {
        try
        {
            // Create a new connection for cleanup
            cleanupConnection = new NatsConnection(NatsOpts.Default with
            {
                Url = Environment.GetEnvironmentVariable("NATS_CONNECTION_STRING") ?? "nats://nats:4222"
            });

            await cleanupConnection.ConnectAsync();
            var jetStream = new NatsJSContext(cleanupConnection);

            // Delete all streams with this test's prefix
            var streams = new List<string>();
            await foreach (var stream in jetStream.ListStreamsAsync())
            {
                if (stream.Info.Config.Name.StartsWith(streamPrefix!))
                {
                    streams.Add(stream.Info.Config.Name);
                }
            }

            foreach (var streamName in streams)
            {
                try
                {
                    await jetStream.DeleteStreamAsync(streamName);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Unable to delete stream {streamName}: {ex.Message}");
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error during cleanup: {ex.Message}");
        }
        finally
        {
            if (cleanupConnection != null)
            {
                await cleanupConnection.DisposeAsync();
            }
        }
    }
}
