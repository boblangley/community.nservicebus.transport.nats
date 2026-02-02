namespace NServiceBus;

using System.Text;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NServiceBus.Extensibility;
using NServiceBus.Transport;

sealed class MessagePump : IMessageReceiver, IDisposable
{
    readonly string id;
    readonly string receiveAddress;
    readonly TopologyManager topologyManager;
    readonly SubscriptionManager subscriptionManager;
    readonly Action<string, Exception, CancellationToken> criticalErrorAction;
    readonly ConnectionFailedCircuitBreaker circuitBreaker;

    OnMessage onMessage = null!;
    OnError onError = null!;
    int maxConcurrency;
    SemaphoreSlim? concurrencyLimiter;
    CancellationTokenSource? messageReceivingCancellationTokenSource;
    CancellationTokenSource? messageProcessingCancellationTokenSource;
    CancellationTokenSource? consumerIterationCancellationTokenSource;
    Task? messageReceivingTask;
    long messagesBeingProcessed;

    public MessagePump(
        string id,
        string receiveAddress,
        TopologyManager topologyManager,
        Action<string, Exception, CancellationToken> criticalErrorAction,
        TimeSpan? circuitBreakerTimeout = null,
        ILoggerFactory? loggerFactory = null)
    {
        this.id = id;
        this.receiveAddress = receiveAddress;
        this.topologyManager = topologyManager;
        this.criticalErrorAction = criticalErrorAction;

        circuitBreaker = new ConnectionFailedCircuitBreaker(
            $"MessagePump-{receiveAddress}",
            circuitBreakerTimeout ?? TimeSpan.FromMinutes(2),
            criticalErrorAction,
            loggerFactory);

        // Create the subscription manager with callback to restart consumer when subscriptions change
        subscriptionManager = new SubscriptionManager(receiveAddress, topologyManager);
        subscriptionManager.SetSubscriptionChangedCallback(OnSubscriptionChanged);
    }

    void OnSubscriptionChanged()
    {
        // Cancel the current consumer iteration to pick up updated filter subjects
        // The while loop in ReceiveMessages will restart and re-fetch the consumer
        consumerIterationCancellationTokenSource?.Cancel();
    }

    public ISubscriptionManager Subscriptions => subscriptionManager;

    public string Id => id;

    public string ReceiveAddress => receiveAddress;

    public Task Initialize(
        PushRuntimeSettings limitations,
        OnMessage onMessage,
        OnError onError,
        CancellationToken cancellationToken = default)
    {
        this.onMessage = onMessage;
        this.onError = onError;
        this.maxConcurrency = limitations.MaxConcurrency;

        return Task.CompletedTask;
    }

    public Task StartReceive(CancellationToken cancellationToken = default)
    {
        messageReceivingCancellationTokenSource = new CancellationTokenSource();
        messageProcessingCancellationTokenSource = new CancellationTokenSource();
        concurrencyLimiter = new SemaphoreSlim(maxConcurrency, maxConcurrency);

        // Single consumer loop handles all messages (unicast, scheduled, and events)
        // Events are captured by the endpoint stream via subject routing
        messageReceivingTask = ReceiveMessages(messageReceivingCancellationTokenSource.Token);

        return Task.CompletedTask;
    }

    public async Task StopReceive(CancellationToken cancellationToken = default)
    {
        if (messageReceivingCancellationTokenSource == null)
        {
            return;
        }

        // Stop receiving new messages
        await messageReceivingCancellationTokenSource.CancelAsync();

        // Register to cancel message processing when the cancellation token is triggered
        await using (cancellationToken.Register(() => messageProcessingCancellationTokenSource?.Cancel()))
        {
            // Wait for all in-flight messages to complete
            while (Interlocked.Read(ref messagesBeingProcessed) > 0)
            {
                await Task.Delay(50, CancellationToken.None);
            }
        }

        // Wait for the receiving task to complete
        if (messageReceivingTask != null)
        {
            try
            {
                await messageReceivingTask.WaitAsync(CancellationToken.None);
            }
            catch (OperationCanceledException)
            {
                // Expected
            }
        }

        concurrencyLimiter?.Dispose();
        messageReceivingCancellationTokenSource?.Dispose();
        messageProcessingCancellationTokenSource?.Dispose();
        consumerIterationCancellationTokenSource?.Dispose();

        messageReceivingCancellationTokenSource = null;
        messageProcessingCancellationTokenSource = null;
        consumerIterationCancellationTokenSource = null;
    }

    public async Task ChangeConcurrency(PushRuntimeSettings limitations, CancellationToken cancellationToken = default)
    {
        // Stop and restart the pump with new concurrency settings
        await StopReceive(cancellationToken);
        maxConcurrency = limitations.MaxConcurrency;
        await StartReceive(cancellationToken);
    }

    async Task ReceiveMessages(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                // Create a linked token that cancels when either:
                // 1. The message pump is stopping (cancellationToken)
                // 2. Subscriptions changed (consumerIterationCancellationTokenSource)
                consumerIterationCancellationTokenSource?.Dispose();
                consumerIterationCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                var iterationToken = consumerIterationCancellationTokenSource.Token;

                var consumer = await topologyManager.GetConsumer(receiveAddress, iterationToken);

                await foreach (var msg in consumer.ConsumeAsync<byte[]>(cancellationToken: iterationToken))
                {
                    // Successfully received a message - disarm circuit breaker
                    circuitBreaker.Success();
                    await ProcessIncomingMessage(msg, cancellationToken);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Expected during shutdown
                break;
            }
            catch (OperationCanceledException)
            {
                // Subscription changed - restart the loop with updated consumer filter
                continue;
            }
            catch (Exception ex)
            {
                // Connection or consumer error - arm circuit breaker
                try
                {
                    await circuitBreaker.Failure(ex, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }
    }

    async Task ProcessIncomingMessage(INatsJSMsg<byte[]> msg, CancellationToken cancellationToken)
    {
        // Wait for a concurrency slot
        await concurrencyLimiter!.WaitAsync(cancellationToken);

        // Track that we're processing a message
        Interlocked.Increment(ref messagesBeingProcessed);

        // Process message without awaiting - allow concurrent processing
        _ = ProcessMessageWithConcurrencyTracking(msg, messageProcessingCancellationTokenSource!.Token);
    }

    async Task ProcessMessageWithConcurrencyTracking(INatsJSMsg<byte[]> msg, CancellationToken cancellationToken)
    {
        try
        {
            await ProcessMessage(msg, cancellationToken);
        }
        finally
        {
            concurrencyLimiter?.Release();
            Interlocked.Decrement(ref messagesBeingProcessed);
        }
    }

    async Task ProcessMessage(INatsJSMsg<byte[]> msg, CancellationToken cancellationToken)
    {
        var originalHeaders = ConvertHeaders(msg.Headers);
        var messageId = GetMessageId(msg, originalHeaders);
        var body = new ReadOnlyMemory<byte>(msg.Data ?? []);

        // Start receive activity for OpenTelemetry
        var streamName = msg.Metadata?.Stream ?? "unknown";
        var consumerName = msg.Metadata?.Consumer ?? "unknown";
        var subject = msg.Subject;

        using var activity = NatsTransportDiagnostics.StartReceiveActivity(
            receiveAddress,
            streamName,
            consumerName,
            subject,
            messageId,
            body.Length);

        // Check for TTBR expiration
        if (IsMessageExpired(originalHeaders))
        {
            // Message has expired - ack without processing
            await msg.AckAsync(cancellationToken: CancellationToken.None);
            return;
        }

        // Create a copy of headers for message context - handlers may modify them
        var messageHeaders = new Dictionary<string, string>(originalHeaders);
        var contextBag = new ContextBag();
        var messageContext = new MessageContext(
            messageId,
            messageHeaders,
            body,
            new TransportTransaction(),
            receiveAddress,
            contextBag);

        try
        {
            // Don't pass the pump's cancellation token to onMessage -
            // in-flight messages should complete even during shutdown unless
            // explicitly cancelled via StopReceive's cancellation token
            await onMessage(messageContext, cancellationToken);
            await msg.AckAsync(cancellationToken: CancellationToken.None);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Message processing cancelled - leave message for redelivery
            return;
        }
        catch (Exception ex)
        {
            NatsTransportDiagnostics.RecordException(activity, ex);

            var deliveryAttempts = (int)(msg.Metadata?.NumDelivered ?? 1);

            // Use original headers for error context - not the possibly modified ones
            var errorContext = new ErrorContext(
                ex,
                originalHeaders,
                messageId,
                body,
                new TransportTransaction(),
                deliveryAttempts,
                receiveAddress,
                contextBag);

            try
            {
                var result = await onError(errorContext, cancellationToken);

                if (result == ErrorHandleResult.RetryRequired)
                {
                    await msg.NakAsync(delay: TimeSpan.FromSeconds(1), cancellationToken: CancellationToken.None);
                }
                else
                {
                    await msg.AckAsync(cancellationToken: CancellationToken.None);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Error handling cancelled - leave message for redelivery
                return;
            }
            catch (Exception onErrorEx)
            {
                // Error handler failed - invoke critical error and NAK the message
                criticalErrorAction(
                    $"Failed to execute recoverability policy for message with native ID: `{messageId}`",
                    onErrorEx,
                    cancellationToken);
                await msg.NakAsync(cancellationToken: CancellationToken.None);
            }
        }
    }

    static Dictionary<string, string> ConvertHeaders(NatsHeaders? natsHeaders)
    {
        var headers = new Dictionary<string, string>();
        if (natsHeaders == null)
        {
            return headers;
        }

        foreach (var kvp in natsHeaders)
        {
            if (kvp.Value.Count > 0 && kvp.Value[0] != null)
            {
                // Decode both key and value
                headers[DecodeHeaderString(kvp.Key)] = DecodeHeaderString(kvp.Value[0]!);
            }
        }

        return headers;
    }

    static string DecodeHeaderString(string value)
    {
        // Check for MIME-style Base64 encoded value: =?UTF-8?B?...?=
        if (value.StartsWith("=?UTF-8?B?", StringComparison.Ordinal) && value.EndsWith("?=", StringComparison.Ordinal))
        {
            var base64 = value[10..^2];
            var bytes = Convert.FromBase64String(base64);
            return Encoding.UTF8.GetString(bytes);
        }
        return value;
    }

    static string GetMessageId(INatsJSMsg<byte[]> msg, Dictionary<string, string> headers)
    {
        // Try to get message ID from Nats-Msg-Id header first
        if (headers.TryGetValue("Nats-Msg-Id", out var msgId) && !string.IsNullOrEmpty(msgId))
        {
            return msgId;
        }

        // Fallback to NServiceBus.MessageId header
        if (headers.TryGetValue("NServiceBus.MessageId", out var nsbMsgId) && !string.IsNullOrEmpty(nsbMsgId))
        {
            return nsbMsgId;
        }

        // Generate a new ID as last resort
        return Guid.NewGuid().ToString();
    }

    static bool IsMessageExpired(Dictionary<string, string> headers)
    {
        // Don't honor TTBR for error messages - they should always be processed
        // Error messages have the NServiceBus.FailedQ header
        if (headers.ContainsKey("NServiceBus.FailedQ"))
        {
            return false;
        }

        // Don't honor TTBR for audit messages - they should always be processed
        // Audit messages have the NServiceBus.ProcessingEnded header
        if (headers.ContainsKey("NServiceBus.ProcessingEnded"))
        {
            return false;
        }

        if (!headers.TryGetValue(MessageDispatcher.TimeToBeReceivedHeader, out var expiresAtValue))
        {
            return false;
        }

        if (!long.TryParse(expiresAtValue, out var expiresAtMs))
        {
            return false;
        }

        var expiresAt = DateTimeOffset.FromUnixTimeMilliseconds(expiresAtMs);
        return DateTimeOffset.UtcNow > expiresAt;
    }

    public void Dispose()
    {
        circuitBreaker.Dispose();
    }
}
