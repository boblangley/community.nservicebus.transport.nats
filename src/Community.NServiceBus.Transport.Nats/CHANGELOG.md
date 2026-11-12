# Changelog

## 2.0.1

### Fixes

- Update NServiceBus, NATS.Net, and Microsoft dependencies. Validate delayed delivery against NATS 2.15.2, prevent JetStream control headers from cancelling retries, and keep concurrently published event types on one source cursor.

## 2.0.0

Released 2026-02-03. These historical notes were reconstructed from the
[tagged source](https://github.com/boblangley/community.nservicebus.transport.nats/tree/2.0.0),
merged PRs, and the closed issue linked below.

### Breaking changes

- Require NATS Server 2.12 or later for native scheduled delivery, with a server-version check at connection time. The 1.x examples used NATS 2.10. ([#6](https://github.com/boblangley/community.nservicebus.transport.nats/pull/6))
- Replace the polling delayed-delivery processor and separate subscription consumers with JetStream sourcing: endpoint streams receive subscribed events from a central events stream and scheduled messages from a central delayed stream. This changes the stream and consumer topology. ([#6](https://github.com/boblangley/community.nservicebus.transport.nats/pull/6))
- Replace `NatsTransport.ConnectionSettings` and `NatsConnectionSettings` with the `ConfigureNatsOptions` callback for authentication, TLS, and connection options; use `ConnectionName` to name the connection. Remove the transport properties `MaxDeliveryAttempts`, `PrefetchCount`, `EnableMessageDeduplication`, and `DeduplicationWindow`. ([#6](https://github.com/boblangley/community.nservicebus.transport.nats/pull/6))
- Change `NatsHealthCheck` to accept a `NatsTransport` instead of a `NatsConnection`. Replace `IHealthChecksBuilder.AddNats(connection, ...)` with `IServiceCollection.AddNatsHealthCheck(transport, ...)`. ([#6](https://github.com/boblangley/community.nservicebus.transport.nats/pull/6))

### Features

- Add OpenTelemetry publish and receive activities through the `NServiceBus.Transport.Nats` activity source, including message metadata, exceptions, and time-to-be-received expiration events. ([#6](https://github.com/boblangley/community.nservicebus.transport.nats/pull/6), [#7](https://github.com/boblangley/community.nservicebus.transport.nats/pull/7))
- Add `AddNatsTcpHealthProbe`, configurable health-check options, and `NatsTransport.GetConnectionState()` for monitoring. Connection names default to the endpoint name. ([#6](https://github.com/boblangley/community.nservicebus.transport.nats/pull/6))
- Extend transport logging to message dispatch, message processing, stream and consumer creation, subscription changes, expiration, and recoverability failures. Include server version, server name, cluster, and transport settings in startup diagnostics. ([#6](https://github.com/boblangley/community.nservicebus.transport.nats/pull/6), [#7](https://github.com/boblangley/community.nservicebus.transport.nats/pull/7))

### Fixes

- Resolve the delayed-delivery processor conflict when scaling out endpoints by using native server scheduling. Issue #5 was explicitly closed as resolved by PR #6. ([#5](https://github.com/boblangley/community.nservicebus.transport.nats/issues/5), [#6](https://github.com/boblangley/community.nservicebus.transport.nats/pull/6))
- Catch and log exceptions escaping concurrent message processing, including acknowledgment failures, while releasing concurrency slots so unacknowledged messages can be redelivered. ([#7](https://github.com/boblangley/community.nservicebus.transport.nats/pull/7))
- Reject null, empty, or whitespace stream prefixes and prefixes containing `*`, `>`, `.`, or spaces before creating the transport infrastructure. ([#7](https://github.com/boblangley/community.nservicebus.transport.nats/pull/7))

### Maintenance

- Update Microsoft health-check and logging dependencies from 9.0.0 to 10.0.2 and Microsoft.SourceLink.GitHub from 8.0.0 to 10.0.102. ([#4](https://github.com/boblangley/community.nservicebus.transport.nats/pull/4))
- Update checkout to v6, setup-dotnet to v5, and upload-artifact to v6. Add API approval and health-check unit tests, and refresh configuration and operations documentation for the new topology. ([#1](https://github.com/boblangley/community.nservicebus.transport.nats/pull/1), [#2](https://github.com/boblangley/community.nservicebus.transport.nats/pull/2), [#3](https://github.com/boblangley/community.nservicebus.transport.nats/pull/3), [#6](https://github.com/boblangley/community.nservicebus.transport.nats/pull/6))

[Changes since 1.0.1](https://github.com/boblangley/community.nservicebus.transport.nats/compare/1.0.1...2.0.0)
· [Release](https://github.com/boblangley/community.nservicebus.transport.nats/releases/tag/2.0.0)

## 1.0.1

Released 2026-01-28.

### Release history

- Both `1.0.0` and `1.0.1` tag the same commit, [`3219ec4`](https://github.com/boblangley/community.nservicebus.transport.nats/commit/3219ec41a8541488154eab96366847314eedb2c5). There are no source changes between these releases. The original release notes only link to the commit history; they do not record a reason for the separate release.

[Compare with 1.0.0](https://github.com/boblangley/community.nservicebus.transport.nats/compare/1.0.0...1.0.1)
· [Release](https://github.com/boblangley/community.nservicebus.transport.nats/releases/tag/1.0.1)

## 1.0.0

Released 2026-01-28. These historical notes were reconstructed from the
[initial implementation](https://github.com/boblangley/community.nservicebus.transport.nats/commit/eb0f5d626e770c0905a379e2b8a57acec65a031b)
and the [release tag](https://github.com/boblangley/community.nservicebus.transport.nats/tree/1.0.0).

### Features

- Introduce a NATS JetStream transport for NServiceBus 10 on .NET 10, using NATS.Net 2.7.1. The initial development and test setup uses NATS Server 2.10 with JetStream enabled.
- Support point-to-point messaging with durable JetStream consumers and publish/subscribe with polymorphic event delivery.
- Support delayed delivery through a polling processor and discard expired messages using NServiceBus time-to-be-received semantics.
- Support concurrent message processing, `None` and `ReceiveOnly` transaction modes, reconnection handling, and a connection-failure circuit breaker.
- Provide an ASP.NET Core health check for NATS connection status and encode Unicode message headers with MIME-style Base64.

### Maintenance

- Include transport and acceptance tests, configuration and operations guides, architecture documentation, a NATS devcontainer, and CI/release workflows.
- Correct NuGet package project and repository URLs before the release. ([`3219ec4`](https://github.com/boblangley/community.nservicebus.transport.nats/commit/3219ec41a8541488154eab96366847314eedb2c5))

[Initial release history](https://github.com/boblangley/community.nservicebus.transport.nats/commits/1.0.0)
· [Release](https://github.com/boblangley/community.nservicebus.transport.nats/releases/tag/1.0.0)
