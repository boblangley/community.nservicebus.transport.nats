namespace NServiceBus;

using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

sealed class TopologyManager
{
    readonly NatsJSContext jetStream;
    readonly string streamPrefix;
    readonly TimeSpan ackWait;
    readonly ILogger logger;
    readonly ConcurrentDictionary<string, HashSet<string>> endpointSubscriptions = new();
    readonly SemaphoreSlim subscriptionLock = new(1, 1);

    public TopologyManager(NatsJSContext jetStream, string streamPrefix, TimeSpan ackWait, ILoggerFactory? loggerFactory = null)
    {
        this.jetStream = jetStream;
        this.streamPrefix = streamPrefix;
        this.ackWait = ackWait;
        logger = loggerFactory?.CreateLogger<TopologyManager>() ?? NullLogger<TopologyManager>.Instance;
    }

    public string StreamPrefix => streamPrefix;

    /// <summary>
    /// Creates the central events stream that captures all published events.
    /// Endpoint streams source from this stream with filters for their subscribed event types.
    /// </summary>
    public async Task CreateEventsStream(CancellationToken cancellationToken = default)
    {
        var streamName = GetEventsStreamName();

        var streamConfig = new StreamConfig(streamName, [GetEventsSubjectWildcard()])
        {
            // Limits retention - messages stay until max_age or max_msgs
            // Sourcing copies messages to endpoint streams, so this is just a router
            Retention = StreamConfigRetention.Limits,
            Storage = StreamConfigStorage.File,
            MaxMsgs = -1,
            MaxBytes = -1,
            // Aggressive cleanup - messages only need to exist long enough for sourcing
            MaxAge = TimeSpan.FromHours(1),
            DuplicateWindow = TimeSpan.FromMinutes(2)
        };

        try
        {
            await jetStream.CreateStreamAsync(streamConfig, cancellationToken);
            logger.LogInformation("Created events stream '{StreamName}'", streamName);
        }
        catch (NatsJSApiException ex) when (ex.Error.Code == 400 && ex.Error.Description?.Contains("already exists") == true)
        {
            // Stream already exists, update it to ensure config is current
            await jetStream.UpdateStreamAsync(streamConfig, cancellationToken);
            logger.LogDebug("Updated existing events stream '{StreamName}'", streamName);
        }
    }

    /// <summary>
    /// Creates the central delayed stream that handles native NATS scheduling.
    /// Messages are published here with Nats-Schedule headers, then delivered to ready subjects
    /// which are sourced by endpoint streams.
    /// </summary>
    public async Task CreateDelayedStream(CancellationToken cancellationToken = default)
    {
        var streamName = GetDelayedStreamName();

        var streamConfig = new StreamConfig(streamName, [
            GetDelayedSubjectWildcard(),
            GetReadySubjectWildcard()
        ])
        {
            // Limits retention - scheduled messages are purged after delivery
            Retention = StreamConfigRetention.Limits,
            Storage = StreamConfigStorage.File,
            MaxMsgs = -1,
            MaxBytes = -1,
            // Keep ready messages long enough for sourcing to pick them up
            MaxAge = TimeSpan.FromHours(1),
            DuplicateWindow = TimeSpan.FromMinutes(2),
            // Enable native message scheduling (NATS 2.12+, ADR-51)
            AllowMsgSchedules = true
        };

        try
        {
            await jetStream.CreateStreamAsync(streamConfig, cancellationToken);
            logger.LogInformation("Created delayed stream '{StreamName}'", streamName);
        }
        catch (NatsJSApiException ex) when (ex.Error.Code == 400 && ex.Error.Description?.Contains("already exists") == true)
        {
            // Stream already exists, update it to ensure config is current
            await jetStream.UpdateStreamAsync(streamConfig, cancellationToken);
            logger.LogDebug("Updated existing delayed stream '{StreamName}'", streamName);
        }
    }

    /// <summary>
    /// Creates endpoint infrastructure: stream for unicast messages with sources from events and delayed streams.
    /// </summary>
    public async Task CreateEndpointInfrastructure(
        string endpointName,
        CancellationToken cancellationToken = default)
    {
        var streamName = GetStreamName(endpointName);
        var delayedStreamName = GetDelayedStreamName();

        // Endpoint stream captures unicast messages directly.
        // Events and delayed messages come through sourcing from central streams.
        // NO AllowMsgSchedules - endpoint streams use Sources which is mutually exclusive.
        var sources = new List<StreamSource>
        {
            // Always source ready messages from the delayed stream for this endpoint
            new StreamSource
            {
                Name = delayedStreamName,
                FilterSubject = GetReadySubject(endpointName)
            }
        };

        var streamConfig = new StreamConfig(streamName, [GetEndpointSubject(endpointName)])
        {
            // WorkQueue retention: messages are deleted after being acknowledged
            Retention = StreamConfigRetention.Workqueue,
            Storage = StreamConfigStorage.File,
            MaxMsgs = -1,
            MaxBytes = -1,
            MaxAge = TimeSpan.Zero, // No age limit - business messages should not be lost
            DuplicateWindow = TimeSpan.FromMinutes(2),
            // Explicitly disable - endpoint streams use Sources which is mutually exclusive with AllowMsgSchedules
            AllowMsgSchedules = false,
            Sources = sources
        };

        try
        {
            await jetStream.CreateStreamAsync(streamConfig, cancellationToken);
            logger.LogInformation("Created endpoint stream '{StreamName}' for endpoint '{EndpointName}'", streamName, endpointName);
        }
        catch (NatsJSApiException ex) when (ex.Error.Code == 400 && ex.Error.Description?.Contains("already exists") == true)
        {
            // Stream exists - read current config to preserve event sources, then update
            var existingStream = await jetStream.GetStreamAsync(streamName, cancellationToken: cancellationToken);

            // Merge existing event sources with the delayed source
            var existingSources = existingStream.Info.Config.Sources ?? [];
            var mergedSources = new List<StreamSource>(sources);
            foreach (var source in existingSources)
            {
                // Keep event sources, skip if it's a duplicate delayed source
                if (source.Name != delayedStreamName)
                {
                    mergedSources.Add(source);
                }
            }
            // When updating, explicitly disable AllowMsgSchedules (may have been enabled in older versions)
            streamConfig = streamConfig with { Sources = mergedSources, AllowMsgSchedules = false };
            await jetStream.UpdateStreamAsync(streamConfig, cancellationToken);
            logger.LogDebug("Updated existing endpoint stream '{StreamName}' for endpoint '{EndpointName}'", streamName, endpointName);
        }

        // Consumer receives all messages from the endpoint stream
        var consumerName = GetConsumerName(endpointName);
        var consumerConfig = new ConsumerConfig(consumerName)
        {
            DurableName = consumerName,
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            AckWait = ackWait,
            MaxDeliver = -1,
            // Filter to unicast, ready (delayed), and event subjects
            // Sourced messages retain their original subjects
            FilterSubjects = [
                GetEndpointSubject(endpointName),
                GetReadySubject(endpointName), // Sourced delayed messages
                GetEventsSubjectWildcard() // Sourced events retain their original subject
            ]
        };

        try
        {
            await jetStream.CreateConsumerAsync(streamName, consumerConfig, cancellationToken);
            logger.LogInformation("Created consumer '{ConsumerName}' on stream '{StreamName}'", consumerName, streamName);
        }
        catch (NatsJSApiException ex) when (ex.Error.Code == 400 && ex.Error.Description?.Contains("already exists") == true)
        {
            // Consumer exists - update it
            await jetStream.CreateOrUpdateConsumerAsync(streamName, consumerConfig, cancellationToken);
            logger.LogDebug("Updated existing consumer '{ConsumerName}' on stream '{StreamName}'", consumerName, streamName);
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
        var delayedStreamName = GetDelayedStreamName();

        // Source ready messages from the delayed stream
        var sources = new List<StreamSource>
        {
            new StreamSource
            {
                Name = delayedStreamName,
                FilterSubject = GetReadySubject(endpointName)
            }
        };

        var streamConfig = new StreamConfig(streamName, [GetEndpointSubject(endpointName)])
        {
            Retention = StreamConfigRetention.Workqueue,
            Storage = StreamConfigStorage.File,
            MaxMsgs = -1,
            MaxBytes = -1,
            MaxAge = TimeSpan.Zero,
            DuplicateWindow = TimeSpan.FromMinutes(2),
            // Explicitly disable - endpoint streams use Sources which is mutually exclusive with AllowMsgSchedules
            AllowMsgSchedules = false,
            Sources = sources
        };

        try
        {
            await jetStream.CreateStreamAsync(streamConfig, cancellationToken);
            logger.LogDebug("Created stream '{StreamName}' for endpoint '{EndpointName}' (lazy creation)", streamName, endpointName);
        }
        catch (NatsJSApiException ex) when (ex.Error.Code == 400 && ex.Error.Description?.Contains("already exists") == true)
        {
            // Stream already exists, that's fine
        }
    }

    /// <summary>
    /// Subscribes an endpoint to an event type by adding a source filter from the events stream.
    /// The endpoint stream will source messages matching the filter from the central events stream.
    /// </summary>
    public async Task SubscribeToEvent(string endpointName, string eventType, CancellationToken cancellationToken = default)
    {
        await subscriptionLock.WaitAsync(cancellationToken);
        try
        {
            var subscriptions = endpointSubscriptions.GetOrAdd(endpointName, _ => []);
            var eventSubject = GetEventSubject(eventType);

            if (!subscriptions.Add(eventSubject))
            {
                logger.LogDebug("Endpoint '{EndpointName}' already subscribed to '{EventType}'", endpointName, eventType);
                return; // Already subscribed locally
            }

            logger.LogDebug("Subscribing endpoint '{EndpointName}' to event type '{EventType}'", endpointName, eventType);
            await UpdateStreamSources(endpointName, cancellationToken);
        }
        finally
        {
            subscriptionLock.Release();
        }
    }

    /// <summary>
    /// Unsubscribes an endpoint from an event type by removing the source filter.
    /// </summary>
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
                logger.LogDebug("Endpoint '{EndpointName}' not subscribed to '{EventType}', skipping unsubscribe", endpointName, eventType);
                return; // Not subscribed locally
            }

            logger.LogDebug("Unsubscribing endpoint '{EndpointName}' from event type '{EventType}'", endpointName, eventType);
            await UpdateStreamSources(endpointName, cancellationToken);
        }
        finally
        {
            subscriptionLock.Release();
        }
    }

    /// <summary>
    /// Updates the endpoint stream's sources to reflect current subscriptions.
    /// Each subscription becomes a source from the events stream with a filter_subject.
    /// The delayed stream source is always preserved.
    /// </summary>
    async Task UpdateStreamSources(string endpointName, CancellationToken cancellationToken)
    {
        var streamName = GetStreamName(endpointName);
        var eventsStreamName = GetEventsStreamName();
        var delayedStreamName = GetDelayedStreamName();

        // Build sources list - always include the delayed stream source
        var sources = new List<StreamSource>
        {
            new StreamSource
            {
                Name = delayedStreamName,
                FilterSubject = GetReadySubject(endpointName)
            }
        };

        // Add event sources from current subscriptions
        if (endpointSubscriptions.TryGetValue(endpointName, out var subscriptions))
        {
            foreach (var eventSubject in subscriptions)
            {
                sources.Add(new StreamSource
                {
                    Name = eventsStreamName,
                    FilterSubject = eventSubject
                });
            }
        }

        // Read existing stream config and update sources
        INatsJSStream stream;
        try
        {
            stream = await jetStream.GetStreamAsync(streamName, cancellationToken: cancellationToken);
        }
        catch (NatsJSApiException)
        {
            // Stream doesn't exist yet - will be created when endpoint starts
            logger.LogDebug("Stream '{StreamName}' does not exist yet. Subscription will be applied when endpoint starts", streamName);
            return;
        }

        var existingConfig = stream.Info.Config;
        var streamConfig = new StreamConfig(streamName, existingConfig.Subjects ?? [])
        {
            Retention = existingConfig.Retention,
            Storage = existingConfig.Storage,
            MaxMsgs = existingConfig.MaxMsgs,
            MaxBytes = existingConfig.MaxBytes,
            MaxAge = existingConfig.MaxAge,
            DuplicateWindow = existingConfig.DuplicateWindow,
            // Explicitly disable AllowMsgSchedules - endpoint streams use Sources which is mutually exclusive
            AllowMsgSchedules = false,
            Sources = sources
        };

        await jetStream.UpdateStreamAsync(streamConfig, cancellationToken);
    }

    public string GetEventsStreamName() => $"{streamPrefix}-events";

    public string GetDelayedStreamName() => $"{streamPrefix}-delayed";

    public string GetStreamName(string endpointName) => $"{streamPrefix}-{SanitizeName(endpointName)}";

    public string GetEndpointSubject(string endpointName) => $"{streamPrefix}.endpoint.{SanitizeName(endpointName)}";

    public string GetConsumerName(string endpointName) => $"{SanitizeName(endpointName)}-main";

    public string GetEventSubject(string eventType) => $"{streamPrefix}.events.{SanitizeEventType(eventType)}";

    /// <summary>
    /// Gets the wildcard subject for all events.
    /// </summary>
    string GetEventsSubjectWildcard() => $"{streamPrefix}.events.>";

    /// <summary>
    /// Gets the delayed subject for a specific scheduled message.
    /// Messages published here are held by NATS until the scheduled time.
    /// </summary>
    public string GetDelayedSubject(string endpointName, string messageId) =>
        $"{streamPrefix}.delayed.{SanitizeName(endpointName)}.{SanitizeName(messageId)}";

    /// <summary>
    /// Gets the wildcard subject for all delayed messages.
    /// </summary>
    string GetDelayedSubjectWildcard() => $"{streamPrefix}.delayed.>";

    /// <summary>
    /// Gets the ready subject for an endpoint.
    /// Scheduled messages are delivered here when their time comes.
    /// Endpoint streams source from this subject.
    /// </summary>
    public string GetReadySubject(string endpointName) =>
        $"{streamPrefix}.ready.{SanitizeName(endpointName)}";

    /// <summary>
    /// Gets the wildcard subject for all ready messages.
    /// </summary>
    string GetReadySubjectWildcard() => $"{streamPrefix}.ready.>";

    /// <summary>
    /// Sanitizes event type names for use in NATS subjects.
    /// NATS uses + as a wildcard, so we need to replace it (used by nested types).
    /// Also replaces . with - since . is the subject delimiter.
    /// </summary>
    static string SanitizeEventType(string eventType) => eventType.Replace("+", "--").Replace(".", "-");

    static string SanitizeName(string name) => name.ToLowerInvariant().Replace(".", "-");
}
