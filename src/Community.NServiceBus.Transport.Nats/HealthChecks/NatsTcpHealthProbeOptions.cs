namespace NServiceBus;

/// <summary>
/// Options for configuring the NATS TCP health probe
/// </summary>
public sealed class NatsTcpHealthProbeOptions
{
    /// <summary>
    /// The TCP port to listen on. Required.
    /// </summary>
    public int Port { get; set; }

    /// <summary>
    /// The IP address to bind to. Default: "0.0.0.0" (all interfaces)
    /// </summary>
    public string BindAddress { get; set; } = "0.0.0.0";
}
