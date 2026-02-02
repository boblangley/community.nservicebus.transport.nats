namespace NServiceBus;

using NServiceBus.Extensibility;
using NServiceBus.Transport;
using NServiceBus.Unicast.Messages;

sealed class SubscriptionManager : ISubscriptionManager
{
    readonly string endpointName;
    readonly TopologyManager topologyManager;
    Action? onSubscriptionChanged;

    public SubscriptionManager(string endpointName, TopologyManager topologyManager)
    {
        this.endpointName = endpointName;
        this.topologyManager = topologyManager;
    }

    /// <summary>
    /// Sets a callback to be invoked when subscriptions change.
    /// This allows the message pump to restart its consumer iteration to pick up new filter subjects.
    /// </summary>
    public void SetSubscriptionChangedCallback(Action callback)
    {
        onSubscriptionChanged = callback;
    }

    public async Task SubscribeAll(
        MessageMetadata[] eventTypes,
        ContextBag context,
        CancellationToken cancellationToken = default)
    {
        foreach (var eventType in eventTypes)
        {
            var typeName = eventType.MessageType.FullName ?? eventType.MessageType.Name;
            await topologyManager.SubscribeToEvent(endpointName, typeName, cancellationToken);
        }

        // Notify the message pump to restart consumer iteration with updated filter
        onSubscriptionChanged?.Invoke();
    }

    public async Task Unsubscribe(
        MessageMetadata eventType,
        ContextBag context,
        CancellationToken cancellationToken = default)
    {
        var typeName = eventType.MessageType.FullName ?? eventType.MessageType.Name;
        await topologyManager.UnsubscribeFromEvent(endpointName, typeName, cancellationToken);

        // Notify the message pump to restart consumer iteration with updated filter
        onSubscriptionChanged?.Invoke();
    }
}
