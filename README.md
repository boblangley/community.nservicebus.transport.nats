# Community.NServiceBus.Transport.Nats

A NATS JetStream transport for NServiceBus.

This is a community-maintained transport implementation that enables NServiceBus message delivery over [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream).

## Documentation

- [Configuration Guide](docs/configuration.md) - How to configure and use the transport
- [Operations Guide](docs/operations.md) - Deployment, NATS CLI commands, and infrastructure setup
- [Architecture](docs/architecture.md) - Technical implementation details for contributors

## Running tests locally

The tests default to `nats://localhost:4222`. You can override this using the `NATS_CONNECTION_STRING` environment variable.

### Start NATS with JetStream

```bash
docker run -d --name nats -p 4222:4222 -p 8222:8222 nats:2.12-alpine --jetstream -m 8222
```

### Run the tests

```bash
dotnet test src/Community.NServiceBus.Transport.Nats.TransportTests
dotnet test src/Community.NServiceBus.Transport.Nats.AcceptanceTests
```

---

This is an independent, community-developed transport. It is not affiliated with, endorsed by, or supported by [Particular Software](https://particular.net). NServiceBus is a registered trademark of Particular Software.