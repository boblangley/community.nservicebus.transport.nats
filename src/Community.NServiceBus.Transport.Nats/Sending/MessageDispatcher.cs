namespace NServiceBus;

using System.Text;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NServiceBus.DelayedDelivery;
using NServiceBus.Performance.TimeToBeReceived;
using NServiceBus.Transport;

sealed class MessageDispatcher : IMessageDispatcher
{
    public const string TimeToBeReceivedHeader = "NServiceBus.Nats.ExpiresAt";

    // Native NATS scheduling headers (NATS 2.12+, ADR-51)
    const string NatsScheduleHeader = "Nats-Schedule";
    const string NatsScheduleTargetHeader = "Nats-Schedule-Target";

    readonly NatsJSContext jetStream;
    readonly TopologyManager topologyManager;

    public MessageDispatcher(NatsJSContext jetStream, TopologyManager topologyManager)
    {
        this.jetStream = jetStream;
        this.topologyManager = topologyManager;
    }

    public async Task Dispatch(
        TransportOperations outgoingMessages,
        TransportTransaction transaction,
        CancellationToken cancellationToken = default)
    {
        foreach (var unicastOperation in outgoingMessages.UnicastTransportOperations)
        {
            await SendUnicast(unicastOperation, cancellationToken);
        }

        foreach (var multicastOperation in outgoingMessages.MulticastTransportOperations)
        {
            await SendMulticast(multicastOperation, cancellationToken);
        }
    }

    async Task SendUnicast(UnicastTransportOperation operation, CancellationToken cancellationToken)
    {
        var headers = ConvertHeaders(operation.Message.Headers);
        var messageId = operation.Message.MessageId;

        // Check if this is a message being moved to the error queue
        // Error queue messages have the NServiceBus.FailedQ header
        // We need a unique ID to avoid deduplication with the original message
        var isErrorQueueMessage = operation.Message.Headers.ContainsKey("NServiceBus.FailedQ");

        // Set message ID for deduplication
        headers["Nats-Msg-Id"] = isErrorQueueMessage
            ? $"{messageId}-error-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}"
            : messageId;

        // Check for delayed delivery
        var deliverAt = GetDeliverAt(operation.Properties);

        // Check for TTBR
        var ttbr = operation.Properties.DiscardIfNotReceivedBefore;
        if (ttbr != null && ttbr.MaxTime < TimeSpan.MaxValue)
        {
            if (deliverAt.HasValue)
            {
                throw new InvalidOperationException(
                    "Delayed delivery of messages with TimeToBeReceived set is not supported. " +
                    "Remove the TimeToBeReceived attribute to delay messages of this type.");
            }

            var expiresAt = DateTimeOffset.UtcNow + ttbr.MaxTime;
            headers[TimeToBeReceivedHeader] = expiresAt.ToUnixTimeMilliseconds().ToString();
        }

        if (deliverAt.HasValue)
        {
            await SendDelayed(
                operation.Destination,
                operation.Message.Body,
                headers,
                deliverAt.Value,
                cancellationToken);
            return;
        }

        var subject = topologyManager.GetEndpointSubject(operation.Destination);
        await PublishWithStreamCreation(
            subject,
            operation.Message.Body.ToArray(),
            headers,
            operation.Destination,
            cancellationToken);
    }

    async Task PublishWithStreamCreation(
        string subject,
        byte[] body,
        NatsHeaders headers,
        string destination,
        CancellationToken cancellationToken)
    {
        var messageId = headers.TryGetValue("Nats-Msg-Id", out var msgIdValues) && msgIdValues.Count > 0
            ? msgIdValues[0]
            : null;

        using var activity = NatsTransportDiagnostics.StartPublishActivity(
            destination,
            subject,
            messageId,
            body.Length);

        try
        {
            await jetStream.PublishAsync(subject, body, headers: headers, cancellationToken: cancellationToken);
        }
        catch (NatsJSApiException ex) when (ex.Error.Code == 10504) // Stream not found
        {
            // Stream doesn't exist - create it and retry
            await topologyManager.EnsureEndpointStreamExists(destination, cancellationToken);
            await jetStream.PublishAsync(subject, body, headers: headers, cancellationToken: cancellationToken);
        }
        catch (NatsJSPublishNoResponseException)
        {
            // No stream matched the subject - create it and retry
            await topologyManager.EnsureEndpointStreamExists(destination, cancellationToken);
            await jetStream.PublishAsync(subject, body, headers: headers, cancellationToken: cancellationToken);
        }
        catch (Exception ex)
        {
            NatsTransportDiagnostics.RecordException(activity, ex);
            throw;
        }
    }

    async Task SendMulticast(MulticastTransportOperation operation, CancellationToken cancellationToken)
    {
        var headers = ConvertHeaders(operation.Message.Headers);
        var messageId = operation.Message.MessageId;

        // Note: Nats-Msg-Id is set per-subject below to avoid JetStream deduplication
        // across different subjects in the type hierarchy

        // Get all types in the hierarchy for polymorphic publishing
        var typeHierarchy = GetTypeHierarchy(operation.MessageType);

        // Check for TTBR
        var ttbr = operation.Properties.DiscardIfNotReceivedBefore;
        if (ttbr != null && ttbr.MaxTime < TimeSpan.MaxValue)
        {
            var expiresAt = DateTimeOffset.UtcNow + ttbr.MaxTime;
            headers[TimeToBeReceivedHeader] = expiresAt.ToUnixTimeMilliseconds().ToString();
        }

        // Publish to all types in the hierarchy for polymorphic subscriptions
        // All messages have the same NServiceBus.MessageId for Outbox deduplication
        var body = operation.Message.Body.ToArray();

        foreach (var typeName in typeHierarchy)
        {
            var subject = topologyManager.GetEventSubject(typeName);

            // Create a fresh copy of headers for each publish (NatsHeaders becomes read-only after publish)
            var publishHeaders = CloneHeaders(headers);

            // Use unique Nats-Msg-Id per subject to avoid JetStream deduplication
            // Format: {messageId}-{sanitized-type-name}
            // The NServiceBus.MessageId header remains the same for Outbox deduplication
            var sanitizedTypeName = typeName.Replace(".", "-").Replace("+", "--");
            var natsMsgId = $"{messageId}-{sanitizedTypeName}";
            publishHeaders["Nats-Msg-Id"] = natsMsgId;

            using var activity = NatsTransportDiagnostics.StartPublishActivity(
                typeName,
                subject,
                natsMsgId,
                body.Length);

            try
            {
                await jetStream.PublishAsync(
                    subject,
                    body,
                    headers: publishHeaders,
                    cancellationToken: cancellationToken);
            }
            catch (Exception ex)
            {
                NatsTransportDiagnostics.RecordException(activity, ex);
                throw;
            }
        }
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

    /// <summary>
    /// Gets all types in the hierarchy that should receive this event.
    /// Includes the concrete type, all base classes (except object), and all interfaces.
    /// </summary>
    static List<string> GetTypeHierarchy(Type messageType)
    {
        var types = new List<string>();

        // Add the concrete type first
        var primaryName = messageType.FullName ?? messageType.Name;
        types.Add(primaryName);

        // Add base classes (excluding System.Object)
        var baseType = messageType.BaseType;
        while (baseType != null && baseType != typeof(object))
        {
            var baseName = baseType.FullName ?? baseType.Name;
            if (!types.Contains(baseName))
            {
                types.Add(baseName);
            }
            baseType = baseType.BaseType;
        }

        // Add all interfaces
        foreach (var interfaceType in messageType.GetInterfaces())
        {
            var interfaceName = interfaceType.FullName ?? interfaceType.Name;
            if (!types.Contains(interfaceName))
            {
                types.Add(interfaceName);
            }
        }

        return types;
    }

    async Task SendDelayed(
        string destination,
        ReadOnlyMemory<byte> body,
        NatsHeaders headers,
        DateTimeOffset deliverAt,
        CancellationToken cancellationToken)
    {
        // Use native NATS scheduling (NATS 2.12+, ADR-51)
        // The message is published to a unique schedule subject within the destination's stream.
        // ADR-51 requirement: Nats-Schedule-Target must point to a subject in the SAME stream.
        // NATS server holds the message and automatically delivers it to the target
        // subject at the scheduled time, then purges the schedule entry.

        var messageId = headers.TryGetValue("Nats-Msg-Id", out var msgIdValues) && msgIdValues.Count > 0
            ? msgIdValues[0]
            : Guid.NewGuid().ToString();

        // Generate a unique schedule message ID to avoid JetStream deduplication
        // This is critical for delayed retries - each retry is a new scheduled message
        var scheduleMsgId = $"{messageId}-sched-{deliverAt.ToUnixTimeMilliseconds()}";

        // Schedule subject is within the destination endpoint's stream (same stream as target)
        var scheduleSubject = topologyManager.GetScheduleSubject(destination, scheduleMsgId);

        // Target subject where the message will be delivered at the scheduled time
        var targetSubject = topologyManager.GetEndpointSubject(destination);

        // Set native scheduling headers
        // Format: @at {RFC3339 timestamp}
        headers[NatsScheduleHeader] = $"@at {deliverAt.UtcDateTime:O}";
        headers[NatsScheduleTargetHeader] = targetSubject;

        // Make sure Nats-Msg-Id is unique for each scheduled message
        headers["Nats-Msg-Id"] = scheduleMsgId;

        await jetStream.PublishAsync(
            scheduleSubject,
            body.ToArray(),
            headers: headers,
            cancellationToken: cancellationToken);
    }

    static DateTimeOffset? GetDeliverAt(DispatchProperties properties)
    {
        if (properties.DoNotDeliverBefore != null)
        {
            return properties.DoNotDeliverBefore.At;
        }

        if (properties.DelayDeliveryWith != null)
        {
            return DateTimeOffset.UtcNow + properties.DelayDeliveryWith.Delay;
        }

        return null;
    }

    static NatsHeaders ConvertHeaders(Dictionary<string, string> nsbHeaders)
    {
        var headers = new NatsHeaders();
        foreach (var kvp in nsbHeaders)
        {
            // Skip null values - some headers may be intentionally set to null
            if (kvp.Value == null)
            {
                continue;
            }

            // Encode both key and value if they contain non-ASCII characters
            headers[EncodeHeaderString(kvp.Key)] = EncodeHeaderString(kvp.Value);
        }
        return headers;
    }

    static string EncodeHeaderString(string value)
    {
        // Check if string contains characters that need encoding:
        // - Non-ASCII characters (> 127)
        // - Spaces (not allowed in NATS header keys)
        // - Colons (not allowed in NATS header keys)
        // - Non-printable characters (< 32)
        foreach (var c in value)
        {
            if (c > 127 || c == ' ' || c == ':' || c < 32)
            {
                // Contains invalid characters, use Base64 encoding with MIME-style prefix
                var bytes = Encoding.UTF8.GetBytes(value);
                return $"=?UTF-8?B?{Convert.ToBase64String(bytes)}?=";
            }
        }
        return value;
    }
}
