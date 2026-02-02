# Contributing to Community.NServiceBus.Transport.Nats

Thank you for your interest in contributing! This document provides guidelines for contributing to the NATS JetStream transport for NServiceBus.

## Getting Started

### Prerequisites

- .NET 10.0 SDK or later
- Docker (for running NATS locally)
- A code editor (VS Code, Visual Studio, or Rider)

### Development Environment

The easiest way to get started is using the included devcontainer:

1. Open the repository in VS Code
2. When prompted, click "Reopen in Container"
3. The container includes NATS with JetStream pre-configured

Alternatively, start NATS manually:

```bash
docker run -d --name nats -p 4222:4222 -p 8222:8222 nats:2.12-alpine --jetstream -m 8222
```

### Running Tests

```bash
# Transport tests (unit/integration)
dotnet test src/Community.NServiceBus.Transport.Nats.TransportTests

# Acceptance tests (end-to-end)
dotnet test src/Community.NServiceBus.Transport.Nats.AcceptanceTests
```

## How to Contribute

### Reporting Issues

- Check existing issues before creating a new one
- Include steps to reproduce the problem
- Include NATS server version and .NET version
- Include relevant error messages and stack traces

### Submitting Pull Requests

1. Fork the repository
2. Create a feature branch from `main`
3. Make your changes
4. Ensure all tests pass
5. Submit a pull request

### Code Guidelines

- Follow existing code style and patterns
- Add tests for new functionality
- Update documentation as needed
- Keep commits focused and atomic

### Commit Messages

Write clear, concise commit messages that explain the "why" rather than the "what":

```
Add support for custom consumer configuration

Allows users to specify additional consumer options like
max pending messages and rate limiting.
```

## Architecture Overview

See [docs/architecture.md](docs/architecture.md) for details on the transport implementation.

## Questions?

Feel free to open an issue for questions or discussions about potential contributions.
