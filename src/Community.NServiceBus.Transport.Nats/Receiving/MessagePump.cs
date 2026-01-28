namespace NServiceBus;

using System.Text;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NServiceBus.Extensibility;
using NServiceBus.Transport;

sealed class MessagePump : IMessageReceiver, IDisposable
{
    readonly string id;
    readonly string receiveAddress;
    readonly TopologyManager topologyManager;
    readonly NatsJSContext jetStream;
    readonly SubscriptionManager subscriptionManager;
    readonly Action<string, Exception, CancellationToken> criticalErrorAction;
    readonly ConnectionFailedCircuitBreaker circuitBreaker;

    OnMessage onMessage = null!;
    OnError onError = null!;
    int maxConcurrency;
    SemaphoreSlim? concurrencyLimiter;
    CancellationTokenSource? messageReceivingCancellationTokenSource;
    CancellationTokenSource? messageProcessingCancellationTokenSource;
    CancellationTokenSource? eventsConsumerCancellationTokenSource;
    Task? messageReceivingTask;
    Task? eventsReceivingTask;
    long messagesBeingProcessed;

    public MessagePump(
        string id,
        string receiveAddress,
        TopologyManager topologyManager,
        NatsJSContext jetStream,
        Action<string, Exception, CancellationToken> criticalErrorAction,
        TimeSpan? circuitBreakerTimeout = null,
        ILoggerFactory? loggerFactory = null)
    {
        this.id = id;
        this.receiveAddress = receiveAddress;
        this.topologyManager = topologyManager;
        this.jetStream = jetStream;
        this.criticalErrorAction = criticalErrorAction;

        circuitBreaker = new ConnectionFailedCircuitBreaker(
            $"MessagePump-{receiveAddress}",
            circuitBreakerTimeout ?? TimeSpan.FromMinutes(2),
            criticalErrorAction,
            loggerFactory);

        // Create the subscription manager with a callback to this pump
        this.subscriptionManager = new SubscriptionManager(receiveAddress, topologyManager, OnSubscriptionChanged);
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
        eventsConsumerCancellationTokenSource = new CancellationTokenSource();
        concurrencyLimiter = new SemaphoreSlim(maxConcurrency, maxConcurrency);

        // Start main queue consumer
        messageReceivingTask = ReceiveFromQueue(messageReceivingCancellationTokenSource.Token);

        // Start events consumer (for pub/sub)
        eventsReceivingTask = ReceiveFromEvents(eventsConsumerCancellationTokenSource.Token);

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
        await eventsConsumerCancellationTokenSource!.CancelAsync();

        // Register to cancel message processing when the cancellation token is triggered
        await using (cancellationToken.Register(() => messageProcessingCancellationTokenSource?.Cancel()))
        {
            // Wait for all in-flight messages to complete
            while (Interlocked.Read(ref messagesBeingProcessed) > 0)
            {
                await Task.Delay(50, CancellationToken.None);
            }
        }

        // Wait for the receiving tasks to complete
        var tasks = new List<Task>();
        if (messageReceivingTask != null) tasks.Add(messageReceivingTask);
        if (eventsReceivingTask != null) tasks.Add(eventsReceivingTask);

        try
        {
            await Task.WhenAll(tasks).WaitAsync(CancellationToken.None);
        }
        catch (OperationCanceledException)
        {
            // Expected
        }

        concurrencyLimiter?.Dispose();
        messageReceivingCancellationTokenSource?.Dispose();
        messageProcessingCancellationTokenSource?.Dispose();
        eventsConsumerCancellationTokenSource?.Dispose();

        messageReceivingCancellationTokenSource = null;
        messageProcessingCancellationTokenSource = null;
        eventsConsumerCancellationTokenSource = null;
    }

    public async Task ChangeConcurrency(PushRuntimeSettings limitations, CancellationToken cancellationToken = default)
    {
        // Stop and restart the pump with new concurrency settings
        await StopReceive(cancellationToken);
        maxConcurrency = limitations.MaxConcurrency;
        await StartReceive(cancellationToken);
    }

    public async Task OnSubscriptionChanged()
    {
        // Restart the events consumer to pick up new subscriptions
        if (eventsConsumerCancellationTokenSource != null)
        {
            await eventsConsumerCancellationTokenSource.CancelAsync();

            if (eventsReceivingTask != null)
            {
                try
                {
                    await eventsReceivingTask;
                }
                catch (OperationCanceledException)
                {
                    // Expected
                }
            }

            eventsConsumerCancellationTokenSource = new CancellationTokenSource();
            eventsReceivingTask = ReceiveFromEvents(eventsConsumerCancellationTokenSource.Token);
        }
    }

    async Task ReceiveFromQueue(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var consumer = await topologyManager.GetConsumer(receiveAddress, cancellationToken);

                await foreach (var msg in consumer.ConsumeAsync<byte[]>(cancellationToken: cancellationToken))
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

    async Task ReceiveFromEvents(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                // Check if there are any event subscriptions
                var eventsConsumer = await topologyManager.GetEventsConsumer(receiveAddress, cancellationToken);
                if (eventsConsumer == null)
                {
                    // No subscriptions, wait until cancelled
                    await Task.Delay(Timeout.Infinite, cancellationToken);
                    return;
                }

                // Use FetchAsync with idle heartbeat to continuously poll for messages
                while (!cancellationToken.IsCancellationRequested)
                {
                    await foreach (var msg in eventsConsumer.FetchAsync<byte[]>(
                        new NatsJSFetchOpts { MaxMsgs = 100, IdleHeartbeat = TimeSpan.FromSeconds(5) },
                        cancellationToken: cancellationToken))
                    {
                        // Successfully received an event - disarm circuit breaker
                        circuitBreaker.Success();
                        await ProcessIncomingMessage(msg, cancellationToken);
                    }

                    // Brief delay between fetch calls to avoid tight loop
                    await Task.Delay(100, cancellationToken);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Expected during shutdown or subscription change
                break;
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

        // Check for TTBR expiration
        if (IsMessageExpired(originalHeaders))
        {
            // Message has expired - ack without processing
            await msg.AckAsync(cancellationToken: CancellationToken.None);
            return;
        }

        var body = new ReadOnlyMemory<byte>(msg.Data ?? []);

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
