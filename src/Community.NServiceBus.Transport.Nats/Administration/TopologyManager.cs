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
        var unicastSubject = GetEndpointSubject(endpointName);
        var scheduleSubjectWildcard = GetScheduleSubjectWildcard(endpointName);

        // Endpoint stream captures unicast messages and schedule subjects
        // Events are handled separately via the events stream
        // Schedule subjects are used for native delayed delivery (NATS 2.12+, ADR-51)
        var streamConfig = new StreamConfig(streamName, [unicastSubject, scheduleSubjectWildcard])
        {
            // WorkQueue retention: messages are deleted after being acknowledged
            Retention = StreamConfigRetention.Workqueue,
            Storage = StreamConfigStorage.File,
            MaxMsgs = -1,
            MaxBytes = -1,
            MaxAge = TimeSpan.Zero, // No age limit - business events should not be lost
            DuplicateWindow = TimeSpan.FromMinutes(2),
            // Enable native message scheduling (NATS 2.12+, ADR-51)
            // Required for delayed delivery - NATS holds the message and delivers at scheduled time
            AllowMsgSchedules = true
        };

        try
        {
            await jetStream.CreateStreamAsync(streamConfig, cancellationToken);
        }
        catch (NatsJSApiException ex) when (ex.Error.Code == 400 && ex.Error.Description?.Contains("already exists") == true)
        {
            await jetStream.UpdateStreamAsync(streamConfig, cancellationToken);
        }

        // Consumer receives unicast messages only (schedule subjects are for triggering delivery)
        var consumerConfig = new ConsumerConfig(GetConsumerName(endpointName))
        {
            DurableName = GetConsumerName(endpointName),
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            AckWait = TimeSpan.FromSeconds(30),
            MaxDeliver = -1,
            FilterSubject = unicastSubject
        };

        try
        {
            await jetStream.CreateOrUpdateConsumerAsync(streamName, consumerConfig, cancellationToken);
        }
        catch (NatsJSApiException ex) when (ex.Error.Code == 400)
        {
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
        var unicastSubject = GetEndpointSubject(endpointName);
        var scheduleSubjectWildcard = GetScheduleSubjectWildcard(endpointName);

        var streamConfig = new StreamConfig(streamName, [unicastSubject, scheduleSubjectWildcard])
        {
            Retention = StreamConfigRetention.Workqueue,
            Storage = StreamConfigStorage.File,
            MaxMsgs = -1,
            MaxBytes = -1,
            MaxAge = TimeSpan.Zero,
            DuplicateWindow = TimeSpan.FromMinutes(2),
            AllowMsgSchedules = true
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

            // Create/update consumer on events stream with filtered subjects
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

            // Update consumer on events stream
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

    /// <summary>
    /// Creates the events stream infrastructure for pub/sub.
    /// This catch-all stream ensures event publishes always succeed, even if no subscribers exist.
    /// Uses Interest retention: messages are deleted when all consumers ACK (no unbounded growth).
    /// </summary>
    public async Task CreateEventsInfrastructure(CancellationToken cancellationToken = default)
    {
        var streamName = GetEventsStreamName();
        var subject = $"{streamPrefix}.events.>";

        var streamConfig = new StreamConfig(streamName, [subject])
        {
            // Interest retention: messages are deleted when all consumers have ACKed
            // If no consumers exist for a subject, the message is immediately deleted
            // This prevents unbounded growth while ensuring publishes always succeed
            Retention = StreamConfigRetention.Interest,
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

    /// <summary>
    /// Gets the schedule subject for a specific message to a specific endpoint.
    /// Each scheduled message needs a unique subject within the endpoint's stream.
    /// The target subject (endpoint subject) must be in the same stream for ADR-51.
    /// </summary>
    public string GetScheduleSubject(string endpointName, string messageId) =>
        $"{streamPrefix}.schedule.{SanitizeName(endpointName)}.{SanitizeName(messageId)}";

    /// <summary>
    /// Gets the wildcard subject pattern for capturing all schedule subjects for an endpoint.
    /// </summary>
    string GetScheduleSubjectWildcard(string endpointName) =>
        $"{streamPrefix}.schedule.{SanitizeName(endpointName)}.>";

    static string SanitizeName(string name) => name.ToLowerInvariant().Replace(".", "-");
}
