namespace NServiceBus;

using System.Collections.Concurrent;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

sealed class TopologyManager
{
    readonly NatsJSContext jetStream;
    readonly string streamPrefix;
    readonly ConcurrentDictionary<string, HashSet<string>> endpointSubscriptions = new();
    readonly SemaphoreSlim subscriptionLock = new(1, 1);

    public TopologyManager(NatsJSContext jetStream, string streamPrefix)
    {
        this.jetStream = jetStream;
        this.streamPrefix = streamPrefix;
    }

    public string StreamPrefix => streamPrefix;

    public async Task CreateEndpointInfrastructure(
        string endpointName,
        CancellationToken cancellationToken = default)
    {
        var streamName = GetStreamName(endpointName);
        var subject = GetEndpointSubject(endpointName);

        var streamConfig = new StreamConfig(streamName, [subject])
        {
            Retention = StreamConfigRetention.Workqueue,
            Storage = StreamConfigStorage.File,
            MaxMsgs = -1,
            MaxBytes = -1,
            MaxAge = TimeSpan.Zero,
            DuplicateWindow = TimeSpan.FromMinutes(2)
        };

        try
        {
            await jetStream.CreateStreamAsync(streamConfig, cancellationToken);
        }
        catch (NatsJSApiException ex) when (ex.Error.Code == 400 && ex.Error.Description?.Contains("already exists") == true)
        {
            // Stream already exists, try to update
            await jetStream.UpdateStreamAsync(streamConfig, cancellationToken);
        }

        var consumerConfig = new ConsumerConfig(GetConsumerName(endpointName))
        {
            DurableName = GetConsumerName(endpointName),
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            AckWait = TimeSpan.FromSeconds(30),
            // MaxDeliver = -1 means unlimited deliveries - let NServiceBus control retry behavior
            MaxDeliver = -1,
            FilterSubject = subject
        };

        try
        {
            await jetStream.CreateOrUpdateConsumerAsync(streamName, consumerConfig, cancellationToken);
        }
        catch (NatsJSApiException ex) when (ex.Error.Code == 400)
        {
            // Consumer exists, update it
            await jetStream.CreateOrUpdateConsumerAsync(streamName, consumerConfig, cancellationToken);
        }
    }

    public async Task<INatsJSConsumer> GetConsumer(string endpointName, CancellationToken cancellationToken = default)
    {
        var streamName = GetStreamName(endpointName);
        var consumerName = GetConsumerName(endpointName);

        return await jetStream.GetConsumerAsync(streamName, consumerName, cancellationToken);
    }

    /// <summary>
    /// Ensures the stream for an endpoint exists. Creates it if it doesn't.
    /// This is used for lazy stream creation when sending to endpoints.
    /// </summary>
    public async Task EnsureEndpointStreamExists(string endpointName, CancellationToken cancellationToken = default)
    {
        var streamName = GetStreamName(endpointName);
        var subject = GetEndpointSubject(endpointName);

        var streamConfig = new StreamConfig(streamName, [subject])
        {
            Retention = StreamConfigRetention.Workqueue,
            Storage = StreamConfigStorage.File,
            MaxMsgs = -1,
            MaxBytes = -1,
            MaxAge = TimeSpan.Zero,
            DuplicateWindow = TimeSpan.FromMinutes(2)
        };

        try
        {
            await jetStream.CreateStreamAsync(streamConfig, cancellationToken);
        }
        catch (NatsJSApiException ex) when (ex.Error.Code == 400 && ex.Error.Description?.Contains("already exists") == true)
        {
            // Stream already exists, that's fine
        }
    }

    public async Task CreateEventsInfrastructure(CancellationToken cancellationToken = default)
    {
        var streamName = GetEventsStreamName();
        var subject = $"{streamPrefix}.events.>";

        // Use Limits retention instead of Interest - Interest requires active consumers
        // and has stricter timing requirements
        var streamConfig = new StreamConfig(streamName, [subject])
        {
            Retention = StreamConfigRetention.Limits,
            Storage = StreamConfigStorage.File,
            MaxMsgs = 10000,
            MaxBytes = -1,
            MaxAge = TimeSpan.FromHours(1),
            DuplicateWindow = TimeSpan.FromMinutes(2)
        };

        try
        {
            await jetStream.CreateStreamAsync(streamConfig, cancellationToken);
        }
        catch (NatsJSApiException ex) when (ex.Error.Code == 400 && ex.Error.Description?.Contains("already exists") == true)
        {
            await jetStream.UpdateStreamAsync(streamConfig, cancellationToken);
        }
    }

    public async Task SubscribeToEvent(string endpointName, string eventType, CancellationToken cancellationToken = default)
    {
        await subscriptionLock.WaitAsync(cancellationToken);
        try
        {
            var subscriptions = endpointSubscriptions.GetOrAdd(endpointName, _ => []);
            var eventSubject = GetEventSubject(eventType);

            if (!subscriptions.Add(eventSubject))
            {
                return; // Already subscribed
            }

            await UpdateEventsConsumer(endpointName, subscriptions, cancellationToken);
        }
        finally
        {
            subscriptionLock.Release();
        }
    }

    public async Task UnsubscribeFromEvent(string endpointName, string eventType, CancellationToken cancellationToken = default)
    {
        await subscriptionLock.WaitAsync(cancellationToken);
        try
        {
            if (!endpointSubscriptions.TryGetValue(endpointName, out var subscriptions))
            {
                return;
            }

            var eventSubject = GetEventSubject(eventType);
            if (!subscriptions.Remove(eventSubject))
            {
                return; // Not subscribed
            }

            await UpdateEventsConsumer(endpointName, subscriptions, cancellationToken);
        }
        finally
        {
            subscriptionLock.Release();
        }
    }

    async Task UpdateEventsConsumer(string endpointName, HashSet<string> subscriptions, CancellationToken cancellationToken)
    {
        var streamName = GetEventsStreamName();
        var consumerName = GetEventsConsumerName(endpointName);

        if (subscriptions.Count == 0)
        {
            // No subscriptions - delete consumer if it exists
            try
            {
                await jetStream.DeleteConsumerAsync(streamName, consumerName, cancellationToken);
            }
            catch (NatsJSApiException ex) when (ex.Error.Code == 404)
            {
                // Consumer doesn't exist
            }
            return;
        }

        var consumerConfig = new ConsumerConfig(consumerName)
        {
            DurableName = consumerName,
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            AckWait = TimeSpan.FromSeconds(30),
            // MaxDeliver = -1 means unlimited deliveries - let NServiceBus control retry behavior
            MaxDeliver = -1,
            FilterSubjects = [.. subscriptions]
        };

        await jetStream.CreateOrUpdateConsumerAsync(streamName, consumerConfig, cancellationToken);
    }

    public async Task<INatsJSConsumer?> GetEventsConsumer(string endpointName, CancellationToken cancellationToken = default)
    {
        if (!endpointSubscriptions.TryGetValue(endpointName, out var subscriptions) || subscriptions.Count == 0)
        {
            return null;
        }

        var streamName = GetEventsStreamName();
        var consumerName = GetEventsConsumerName(endpointName);

        try
        {
            return await jetStream.GetConsumerAsync(streamName, consumerName, cancellationToken);
        }
        catch (NatsJSApiException ex) when (ex.Error.Code == 404)
        {
            return null;
        }
    }

    public bool HasEventSubscriptions(string endpointName) =>
        endpointSubscriptions.TryGetValue(endpointName, out var subs) && subs.Count > 0;

    public string GetStreamName(string endpointName) => $"{streamPrefix}-{SanitizeName(endpointName)}";

    public string GetEndpointSubject(string endpointName) => $"{streamPrefix}.endpoint.{SanitizeName(endpointName)}";

    public string GetConsumerName(string endpointName) => $"{SanitizeName(endpointName)}-main";

    public async Task CreateDelayedDeliveryInfrastructure(CancellationToken cancellationToken = default)
    {
        var streamName = GetDelayedStreamName();
        var subject = $"{streamPrefix}.delayed.>";

        var streamConfig = new StreamConfig(streamName, [subject])
        {
            Retention = StreamConfigRetention.Workqueue,
            Storage = StreamConfigStorage.File,
            MaxMsgs = -1,
            MaxBytes = -1,
            MaxAge = TimeSpan.Zero,
            DuplicateWindow = TimeSpan.FromMinutes(2)
        };

        try
        {
            await jetStream.CreateStreamAsync(streamConfig, cancellationToken);
        }
        catch (NatsJSApiException ex) when (ex.Error.Code == 400 && ex.Error.Description?.Contains("already exists") == true)
        {
            await jetStream.UpdateStreamAsync(streamConfig, cancellationToken);
        }

        // Create a consumer for processing delayed messages
        var consumerConfig = new ConsumerConfig(GetDelayedConsumerName())
        {
            DurableName = GetDelayedConsumerName(),
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            AckWait = TimeSpan.FromSeconds(30),
            MaxDeliver = 10
        };

        await jetStream.CreateOrUpdateConsumerAsync(streamName, consumerConfig, cancellationToken);
    }

    public async Task<INatsJSConsumer> GetDelayedConsumer(CancellationToken cancellationToken = default)
    {
        var streamName = GetDelayedStreamName();
        var consumerName = GetDelayedConsumerName();
        return await jetStream.GetConsumerAsync(streamName, consumerName, cancellationToken);
    }

    public string GetEventsStreamName() => $"{streamPrefix}-events";

    public string GetEventsConsumerName(string endpointName) => $"{SanitizeName(endpointName)}-events";

    public string GetEventSubject(string eventType) => $"{streamPrefix}.events.{SanitizeEventType(eventType)}";

    /// <summary>
    /// Sanitizes event type names for use in NATS subjects.
    /// NATS uses + as a wildcard, so we need to replace it (used by nested types).
    /// Also replaces . with - since . is the subject delimiter.
    /// </summary>
    static string SanitizeEventType(string eventType) => eventType.Replace("+", "--").Replace(".", "-");

    public string GetDelayedStreamName() => $"{streamPrefix}-delayed";

    public string GetDelayedConsumerName() => "delayed-processor";

    public string GetDelayedSubject(string destination) => $"{streamPrefix}.delayed.{SanitizeName(destination)}";

    static string SanitizeName(string name) => name.ToLowerInvariant().Replace(".", "-");
}
