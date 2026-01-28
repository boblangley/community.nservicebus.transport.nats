namespace NServiceBus;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

/// <summary>
/// Circuit breaker that monitors connection failures and triggers critical error after sustained failures.
/// </summary>
/// <remarks>
/// The circuit breaker has three states:
/// - Disarmed: Normal operation, no failures
/// - Armed: Failures occurring, waiting to see if they persist
/// - Triggered: Failures have persisted beyond the timeout, critical error invoked
/// </remarks>
sealed class ConnectionFailedCircuitBreaker : IDisposable
{
    const int Disarmed = 0;
    const int Armed = 1;
    const int Triggered = 2;

    readonly string name;
    readonly TimeSpan timeToWaitBeforeTriggering;
    readonly Action<string, Exception, CancellationToken> criticalErrorAction;
    readonly ILogger logger;
    readonly Timer timer;
    readonly object stateLock = new();
    readonly TimeSpan delayOnFailure;

    int circuitBreakerState = Disarmed;
    Exception? lastException;

    /// <summary>
    /// Creates a new circuit breaker instance
    /// </summary>
    /// <param name="name">Name for logging purposes</param>
    /// <param name="timeToWaitBeforeTriggering">How long to wait after first failure before triggering</param>
    /// <param name="criticalErrorAction">Action to invoke when triggered</param>
    /// <param name="loggerFactory">Optional logger factory</param>
    /// <param name="delayOnFailure">Delay to apply on each failure (default: 1 second)</param>
    public ConnectionFailedCircuitBreaker(
        string name,
        TimeSpan timeToWaitBeforeTriggering,
        Action<string, Exception, CancellationToken> criticalErrorAction,
        ILoggerFactory? loggerFactory = null,
        TimeSpan? delayOnFailure = null)
    {
        this.name = name;
        this.timeToWaitBeforeTriggering = timeToWaitBeforeTriggering;
        this.criticalErrorAction = criticalErrorAction;
        this.delayOnFailure = delayOnFailure ?? TimeSpan.FromSeconds(1);
        logger = loggerFactory?.CreateLogger<ConnectionFailedCircuitBreaker>() ?? NullLogger<ConnectionFailedCircuitBreaker>.Instance;

        timer = new Timer(CircuitBreakerTriggered);
    }

    /// <summary>
    /// Gets whether the circuit breaker is currently disarmed (no failures)
    /// </summary>
    public bool IsDisarmed => Volatile.Read(ref circuitBreakerState) == Disarmed;

    /// <summary>
    /// Gets whether the circuit breaker has been triggered
    /// </summary>
    public bool IsTriggered => Volatile.Read(ref circuitBreakerState) == Triggered;

    /// <summary>
    /// Records a successful operation, disarming the circuit breaker if armed
    /// </summary>
    public void Success()
    {
        if (Volatile.Read(ref circuitBreakerState) == Disarmed)
        {
            return;
        }

        lock (stateLock)
        {
            if (circuitBreakerState == Disarmed)
            {
                return;
            }

            circuitBreakerState = Disarmed;
            timer.Change(Timeout.Infinite, Timeout.Infinite);

            logger.LogInformation("Circuit breaker '{Name}' is now disarmed", name);
        }
    }

    /// <summary>
    /// Records a failure, arming the circuit breaker if disarmed
    /// </summary>
    /// <param name="exception">The exception that caused the failure</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>A task that completes after the failure delay</returns>
    public async Task Failure(Exception exception, CancellationToken cancellationToken = default)
    {
        Interlocked.Exchange(ref lastException, exception);

        var previousState = Volatile.Read(ref circuitBreakerState);
        if (previousState is Armed or Triggered)
        {
            await Task.Delay(delayOnFailure, cancellationToken).ConfigureAwait(false);
            return;
        }

        lock (stateLock)
        {
            if (circuitBreakerState is Armed or Triggered)
            {
                return;
            }

            circuitBreakerState = Armed;
            timer.Change(timeToWaitBeforeTriggering, Timeout.InfiniteTimeSpan);

            logger.LogWarning(
                exception,
                "Circuit breaker '{Name}' is now armed. Will trigger in {Timeout} if not disarmed",
                name,
                timeToWaitBeforeTriggering);
        }

        await Task.Delay(delayOnFailure, cancellationToken).ConfigureAwait(false);
    }

    void CircuitBreakerTriggered(object? state)
    {
        if (Volatile.Read(ref circuitBreakerState) == Disarmed)
        {
            return;
        }

        lock (stateLock)
        {
            if (circuitBreakerState == Disarmed)
            {
                return;
            }

            circuitBreakerState = Triggered;

            var exception = lastException ?? new InvalidOperationException("Connection failed");

            logger.LogError(
                exception,
                "Circuit breaker '{Name}' has been triggered",
                name);

            try
            {
                criticalErrorAction(
                    $"Circuit breaker '{name}' triggered: connection to NATS has failed",
                    exception,
                    CancellationToken.None);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to execute critical error action for circuit breaker '{Name}'", name);
            }
        }
    }

    /// <summary>
    /// Disposes the circuit breaker
    /// </summary>
    public void Dispose()
    {
        timer.Dispose();
    }
}
