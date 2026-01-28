namespace NServiceBus;

using NServiceBus.Extensibility;
using NServiceBus.Transport;
using NServiceBus.Unicast.Messages;

sealed class SubscriptionManager : ISubscriptionManager
{
    readonly string endpointName;
    readonly TopologyManager topologyManager;
    readonly Func<Task> onSubscriptionChanged;

    public SubscriptionManager(string endpointName, TopologyManager topologyManager, Func<Task> onSubscriptionChanged)
    {
        this.endpointName = endpointName;
        this.topologyManager = topologyManager;
        this.onSubscriptionChanged = onSubscriptionChanged;
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

        await onSubscriptionChanged();
    }

    public async Task Unsubscribe(
        MessageMetadata eventType,
        ContextBag context,
        CancellationToken cancellationToken = default)
    {
        var typeName = eventType.MessageType.FullName ?? eventType.MessageType.Name;
        await topologyManager.UnsubscribeFromEvent(endpointName, typeName, cancellationToken);

        await onSubscriptionChanged();
    }
}
