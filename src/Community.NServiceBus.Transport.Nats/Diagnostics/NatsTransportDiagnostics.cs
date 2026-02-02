namespace NServiceBus;

using System.Diagnostics;

/// <summary>
/// OpenTelemetry instrumentation for the NATS transport.
/// Follows OpenTelemetry Semantic Conventions for Messaging.
/// </summary>
public static class NatsTransportDiagnostics
{
    /// <summary>
    /// The ActivitySource name for the NATS transport.
    /// Use this to register with OpenTelemetry: <c>builder.AddSource(NatsTransportDiagnostics.ActivitySourceName)</c>
    /// </summary>
    public const string ActivitySourceName = "NServiceBus.Transport.Nats";

    /// <summary>
    /// The ActivitySource for the NATS transport.
    /// </summary>
    public static ActivitySource ActivitySource { get; } = new(ActivitySourceName, GetVersion());

    static string GetVersion()
    {
        var assembly = typeof(NatsTransportDiagnostics).Assembly;
        var version = assembly.GetName().Version;
        return version?.ToString(3) ?? "0.0.0";
    }

    // OpenTelemetry Semantic Convention attribute names for messaging
    internal static class Attributes
    {
        // Required attributes
        public const string MessagingSystem = "messaging.system";
        public const string MessagingOperationName = "messaging.operation.name";
        public const string MessagingOperationType = "messaging.operation.type";

        // Destination attributes
        public const string MessagingDestinationName = "messaging.destination.name";

        // Message attributes
        public const string MessagingMessageId = "messaging.message.id";
        public const string MessagingMessageConversationId = "messaging.message.conversation_id";
        public const string MessagingMessageBodySize = "messaging.message.body.size";

        // NATS-specific attributes
        public const string MessagingNatsStreamName = "messaging.nats.stream.name";
        public const string MessagingNatsSubject = "messaging.nats.subject";
        public const string MessagingNatsConsumerName = "messaging.nats.consumer.name";

        // Server attributes
        public const string ServerAddress = "server.address";
        public const string ServerPort = "server.port";

        // Error attributes
        public const string ErrorType = "error.type";
    }

    internal static class Values
    {
        public const string MessagingSystemValue = "nats";
        public const string OperationPublish = "publish";
        public const string OperationReceive = "receive";
        public const string OperationProcess = "process";
        public const string OperationTypePublish = "publish";
        public const string OperationTypeReceive = "receive";
        public const string OperationTypeProcess = "process";
    }

    /// <summary>
    /// Creates a publish activity for outgoing messages.
    /// </summary>
    internal static Activity? StartPublishActivity(
        string destination,
        string subject,
        string? messageId,
        int bodySize)
    {
        if (!ActivitySource.HasListeners())
        {
            return null;
        }

        var activity = ActivitySource.StartActivity(
            $"{destination} publish",
            ActivityKind.Producer);

        if (activity == null)
        {
            return null;
        }

        activity.SetTag(Attributes.MessagingSystem, Values.MessagingSystemValue);
        activity.SetTag(Attributes.MessagingOperationName, Values.OperationPublish);
        activity.SetTag(Attributes.MessagingOperationType, Values.OperationTypePublish);
        activity.SetTag(Attributes.MessagingDestinationName, destination);
        activity.SetTag(Attributes.MessagingNatsSubject, subject);
        activity.SetTag(Attributes.MessagingMessageBodySize, bodySize);

        if (!string.IsNullOrEmpty(messageId))
        {
            activity.SetTag(Attributes.MessagingMessageId, messageId);
        }

        return activity;
    }

    /// <summary>
    /// Creates a receive activity for incoming messages.
    /// </summary>
    internal static Activity? StartReceiveActivity(
        string endpoint,
        string streamName,
        string consumerName,
        string subject,
        string? messageId,
        int bodySize)
    {
        if (!ActivitySource.HasListeners())
        {
            return null;
        }

        var activity = ActivitySource.StartActivity(
            $"{endpoint} receive",
            ActivityKind.Consumer);

        if (activity == null)
        {
            return null;
        }

        activity.SetTag(Attributes.MessagingSystem, Values.MessagingSystemValue);
        activity.SetTag(Attributes.MessagingOperationName, Values.OperationReceive);
        activity.SetTag(Attributes.MessagingOperationType, Values.OperationTypeReceive);
        activity.SetTag(Attributes.MessagingDestinationName, endpoint);
        activity.SetTag(Attributes.MessagingNatsStreamName, streamName);
        activity.SetTag(Attributes.MessagingNatsConsumerName, consumerName);
        activity.SetTag(Attributes.MessagingNatsSubject, subject);
        activity.SetTag(Attributes.MessagingMessageBodySize, bodySize);

        if (!string.IsNullOrEmpty(messageId))
        {
            activity.SetTag(Attributes.MessagingMessageId, messageId);
        }

        return activity;
    }

    /// <summary>
    /// Records an error on the current activity.
    /// </summary>
    internal static void RecordException(Activity? activity, Exception exception)
    {
        if (activity == null)
        {
            return;
        }

        activity.SetStatus(ActivityStatusCode.Error, exception.Message);
        activity.SetTag(Attributes.ErrorType, exception.GetType().FullName);
        activity.AddEvent(new ActivityEvent("exception", tags: new ActivityTagsCollection
        {
            { "exception.type", exception.GetType().FullName },
            { "exception.message", exception.Message },
            { "exception.stacktrace", exception.StackTrace }
        }));
    }
}
