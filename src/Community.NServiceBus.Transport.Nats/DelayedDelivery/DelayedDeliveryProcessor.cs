namespace NServiceBus;

using NATS.Client.Core;
using NATS.Client.JetStream;

sealed class DelayedDeliveryProcessor : IDisposable
{
    readonly NatsJSContext jetStream;
    readonly TopologyManager topologyManager;
    readonly TimeSpan pollingInterval;
    CancellationTokenSource? cancellationTokenSource;
    Task? processingTask;

    public DelayedDeliveryProcessor(
        NatsJSContext jetStream,
        TopologyManager topologyManager,
        TimeSpan? pollingInterval = null)
    {
        this.jetStream = jetStream;
        this.topologyManager = topologyManager;
        this.pollingInterval = pollingInterval ?? TimeSpan.FromSeconds(1);
    }

    public Task Start(CancellationToken cancellationToken = default)
    {
        cancellationTokenSource = new CancellationTokenSource();
        processingTask = ProcessDelayedMessages(cancellationTokenSource.Token);
        return Task.CompletedTask;
    }

    public async Task Stop(CancellationToken cancellationToken = default)
    {
        if (cancellationTokenSource == null)
        {
            return;
        }

        await cancellationTokenSource.CancelAsync();

        if (processingTask != null)
        {
            try
            {
                await processingTask.WaitAsync(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                // Expected
            }
        }

        cancellationTokenSource.Dispose();
        cancellationTokenSource = null;
    }

    async Task ProcessDelayedMessages(CancellationToken cancellationToken)
    {
        INatsJSConsumer? consumer = null;

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                consumer ??= await topologyManager.GetDelayedConsumer(cancellationToken);

                // Fetch messages in batches
                await foreach (var msg in consumer.FetchAsync<byte[]>(
                    new NatsJSFetchOpts { MaxMsgs = 100 },
                    cancellationToken: cancellationToken))
                {
                    await ProcessDelayedMessage(msg, cancellationToken);
                }

                // Wait before next poll
                await Task.Delay(pollingInterval, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Expected during shutdown
                break;
            }
            catch (Exception)
            {
                // Log error and retry after delay
                consumer = null; // Reset consumer on error
                try
                {
                    await Task.Delay(pollingInterval, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
    }

    async Task ProcessDelayedMessage(INatsJSMsg<byte[]> msg, CancellationToken cancellationToken)
    {
        var headers = msg.Headers;
        if (headers == null)
        {
            // Invalid message - no headers, ack and discard
            await msg.AckAsync(cancellationToken: CancellationToken.None);
            return;
        }

        // Extract delivery time
        if (!headers.TryGetValue(MessageDispatcher.DelayedDeliveryAtHeader, out var deliveryAtValues) ||
            deliveryAtValues.Count == 0 ||
            !long.TryParse(deliveryAtValues[0], out var deliveryAtMs))
        {
            // Invalid message - no delivery time, ack and discard
            await msg.AckAsync(cancellationToken: CancellationToken.None);
            return;
        }

        var deliveryAt = DateTimeOffset.FromUnixTimeMilliseconds(deliveryAtMs);
        var now = DateTimeOffset.UtcNow;

        if (deliveryAt > now)
        {
            // Not yet time to deliver - NAK with a delay to avoid hitting MaxDeliver limit
            // The delay should be the time until delivery or max 30 seconds
            var timeUntilDelivery = deliveryAt - now;
            var nakDelay = TimeSpan.FromSeconds(Math.Min(timeUntilDelivery.TotalSeconds, 30));
            if (nakDelay < TimeSpan.FromSeconds(1))
            {
                nakDelay = TimeSpan.FromSeconds(1);
            }
            await msg.NakAsync(delay: nakDelay, cancellationToken: CancellationToken.None);
            return;
        }

        // Time to deliver - extract destination and forward
        if (!headers.TryGetValue(MessageDispatcher.DelayedDestinationHeader, out var destinationValues) ||
            destinationValues.Count == 0 ||
            string.IsNullOrEmpty(destinationValues[0]))
        {
            // Invalid message - no destination, ack and discard
            await msg.AckAsync(cancellationToken: CancellationToken.None);
            return;
        }

        var destination = destinationValues[0]!;
        var isMulticast = headers.TryGetValue(MessageDispatcher.DelayedIsMulticastHeader, out var multicastValues) &&
                          multicastValues.Count > 0 &&
                          bool.TryParse(multicastValues[0], out var multicast) &&
                          multicast;

        // Create new headers without the delayed delivery metadata
        var forwardHeaders = new NatsHeaders();
        foreach (var kvp in headers)
        {
            if (kvp.Key != MessageDispatcher.DelayedDeliveryAtHeader &&
                kvp.Key != MessageDispatcher.DelayedDestinationHeader &&
                kvp.Key != MessageDispatcher.DelayedIsMulticastHeader &&
                kvp.Key != MessageDispatcher.DelayedMessageTypeHeader &&
                kvp.Key != "Nats-Msg-Id") // Don't copy the delayed message ID
            {
                foreach (var value in kvp.Value)
                {
                    if (value != null)
                    {
                        forwardHeaders.Add(kvp.Key, value);
                    }
                }
            }
        }

        // Generate a unique message ID for the forwarded message to avoid deduplication issues
        // Use the original NServiceBus message ID with a unique suffix
        var nsbMessageId = forwardHeaders.TryGetValue("NServiceBus.MessageId", out var nsbMsgIdValues) && nsbMsgIdValues.Count > 0
            ? nsbMsgIdValues[0]
            : Guid.NewGuid().ToString();
        forwardHeaders["Nats-Msg-Id"] = $"{nsbMessageId}-fwd-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";

        // Forward the message
        if (isMulticast)
        {
            // Get type hierarchy from header for multicast (semicolon-separated)
            var typeHierarchy = new List<string> { destination };
            if (headers.TryGetValue(MessageDispatcher.DelayedMessageTypeHeader, out var typeValues) &&
                typeValues.Count > 0 &&
                !string.IsNullOrEmpty(typeValues[0]))
            {
                typeHierarchy = typeValues[0]!.Split(';').ToList();
            }

            // Publish to all types in the hierarchy for polymorphic subscriptions
            // Need unique Nats-Msg-Id per subject to avoid JetStream deduplication
            var body = msg.Data ?? [];
            foreach (var typeName in typeHierarchy)
            {
                var subject = topologyManager.GetEventSubject(typeName);

                // Clone headers for each publish (NatsHeaders becomes read-only after publish)
                var publishHeaders = CloneHeaders(forwardHeaders);

                // Use unique Nats-Msg-Id per subject
                var sanitizedTypeName = typeName.Replace(".", "-").Replace("+", "--");
                publishHeaders["Nats-Msg-Id"] = $"{nsbMessageId}-fwd-{sanitizedTypeName}";

                await jetStream.PublishAsync(
                    subject,
                    body,
                    headers: publishHeaders,
                    cancellationToken: cancellationToken);
            }
        }
        else
        {
            // Unicast - forward to single destination with lazy stream creation
            var subject = topologyManager.GetEndpointSubject(destination);
            try
            {
                await jetStream.PublishAsync(
                    subject,
                    msg.Data ?? [],
                    headers: forwardHeaders,
                    cancellationToken: cancellationToken);
            }
            catch (NatsJSApiException ex) when (ex.Error.Code == 10504)
            {
                // Stream doesn't exist - create it and retry
                await topologyManager.EnsureEndpointStreamExists(destination, cancellationToken);
                await jetStream.PublishAsync(
                    subject,
                    msg.Data ?? [],
                    headers: forwardHeaders,
                    cancellationToken: cancellationToken);
            }
            catch (NatsJSPublishNoResponseException)
            {
                // No stream matched the subject - create it and retry
                await topologyManager.EnsureEndpointStreamExists(destination, cancellationToken);
                await jetStream.PublishAsync(
                    subject,
                    msg.Data ?? [],
                    headers: forwardHeaders,
                    cancellationToken: cancellationToken);
            }
        }

        // Ack the delayed message
        await msg.AckAsync(cancellationToken: CancellationToken.None);
    }

    static NatsHeaders CloneHeaders(NatsHeaders source)
    {
        var clone = new NatsHeaders();
        foreach (var kvp in source)
        {
            foreach (var value in kvp.Value)
            {
                if (value != null)
                {
                    clone.Add(kvp.Key, value);
                }
            }
        }
        return clone;
    }

    public void Dispose()
    {
        cancellationTokenSource?.Dispose();
    }
}
