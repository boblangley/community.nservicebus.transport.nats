# Operations Guide

This guide covers deployment, infrastructure setup, and operational tasks for the NATS JetStream transport.

## NATS Server Requirements

### Minimum Version

NATS Server 2.12 or later with JetStream enabled.

### Enabling JetStream

**Command line:**
```bash
nats-server --jetstream
```

**Configuration file:**
```conf
jetstream {
    store_dir: "/data/jetstream"
    max_memory_store: 1GB
    max_file_store: 100GB
}
```

### Docker

```bash
docker run -d \
  --name nats \
  -p 4222:4222 \
  -p 8222:8222 \
  -v nats-data:/data \
  nats:2.12-alpine \
  --jetstream \
  --store_dir /data \
  -m 8222
```

## NATS CLI Setup

Install the NATS CLI for administrative tasks:

```bash
# macOS
brew install nats-io/nats-tools/nats

# Linux (download from releases)
curl -L https://github.com/nats-io/natscli/releases/download/v0.1.5/nats-0.1.5-linux-amd64.zip -o nats.zip
unzip nats.zip
sudo mv nats-0.1.5-linux-amd64/nats /usr/local/bin/
rm -rf nats.zip nats-0.1.5-linux-amd64

# Verify installation
nats --version
```

Configure the context:
```bash
nats context save production --server nats://your-server:4222
nats context select production
```

## Pre-Creating Infrastructure

For least-privilege deployments, create streams and consumers before starting endpoints. This allows the application to run without stream/consumer creation permissions.

### Stream Naming Convention

| Purpose | Stream Name | Subject Pattern |
|---------|-------------|-----------------|
| Endpoint queue | `{prefix}-{endpoint}` | `{prefix}.endpoint.{endpoint}` |
| Events (pub/sub) | `{prefix}-events` | `{prefix}.events.>` |
| Delayed messages | `{prefix}-delayed` | `{prefix}.delayed.>`, `{prefix}.ready.>` |

The default prefix is `nsb`. Endpoint names are lowercased with `.` replaced by `-`.

### Create Endpoint Stream

For each endpoint, create a stream with sources from the events and delayed streams:

```bash
# Variables
PREFIX="nsb"
ENDPOINT="orders"

# Create the endpoint stream with sources
# Note: Sources are configured via the NATS CLI or API
nats stream add ${PREFIX}-${ENDPOINT} \
  --subjects "${PREFIX}.endpoint.${ENDPOINT}" \
  --retention work \
  --storage file \
  --replicas 3 \
  --discard old \
  --max-msgs=-1 \
  --max-bytes=-1 \
  --max-age=0 \
  --duplicate-window=2m \
  --no-deny-delete \
  --no-deny-purge \
  --source "${PREFIX}-delayed" \
  --source-filter-subject "${PREFIX}.ready.${ENDPOINT}"

# Create the consumer (filters to all message types this endpoint receives)
nats consumer add ${PREFIX}-${ENDPOINT} ${ENDPOINT}-main \
  --filter "${PREFIX}.endpoint.${ENDPOINT},${PREFIX}.ready.${ENDPOINT},${PREFIX}.events.>" \
  --ack explicit \
  --wait 30s \
  --max-deliver=-1 \
  --replay instant \
  --deliver all \
  --no-headers-only
```

**Note**: Event subscriptions add additional sources to the endpoint stream at runtime. Each subscribed event type creates a source from `{prefix}-events` with a filter for that event's subject.

### Create Events Stream

Create a single shared stream for all pub/sub events. This acts as a central router - endpoint streams source from it with filters for their subscribed event types.

```bash
PREFIX="nsb"

nats stream add ${PREFIX}-events \
  --subjects "${PREFIX}.events.>" \
  --retention limits \
  --storage file \
  --replicas 3 \
  --discard old \
  --max-msgs=-1 \
  --max-bytes=-1 \
  --max-age=1h \
  --duplicate-window=2m \
  --no-deny-delete \
  --no-deny-purge
```

**Note**: No consumers are created directly on the events stream. Event subscriptions work through stream sourcing - when an endpoint subscribes to an event type, a source is added to the endpoint's stream with a filter for that event subject. The transport manages these sources automatically at runtime.

### Create Delayed Delivery Stream

The delayed stream uses native NATS scheduling (NATS 2.12+, ADR-51). Messages are published with `Nats-Schedule` headers, held by NATS until the scheduled time, then delivered to ready subjects which are sourced by endpoint streams.

```bash
PREFIX="nsb"

# Create the delayed messages stream with native scheduling enabled
# Subjects: delayed.> for incoming scheduled messages, ready.> for delivery
nats stream add ${PREFIX}-delayed \
  --subjects "${PREFIX}.delayed.>,${PREFIX}.ready.>" \
  --retention limits \
  --storage file \
  --replicas 3 \
  --discard old \
  --max-msgs=-1 \
  --max-bytes=-1 \
  --max-age=1h \
  --duplicate-window=2m \
  --no-deny-delete \
  --no-deny-purge \
  --allow-msg-schedules
```

**Note**: No consumer is created on the delayed stream. Endpoint streams source from `{prefix}.ready.{endpoint}` to receive scheduled messages when they become due. The `--allow-msg-schedules` flag enables native NATS scheduling (requires NATS 2.12+).

## Shell Script for Endpoint Setup

Save this as `create-endpoint.sh`:

```bash
#!/bin/bash
set -e

PREFIX="${1:-nsb}"
ENDPOINT="${2:?Endpoint name required}"
REPLICAS="${3:-1}"

echo "Creating infrastructure for endpoint: ${ENDPOINT}"
echo "Prefix: ${PREFIX}, Replicas: ${REPLICAS}"

# Sanitize endpoint name (lowercase, replace . with -)
SANITIZED=$(echo "${ENDPOINT}" | tr '[:upper:]' '[:lower:]' | tr '.' '-')

# Create endpoint stream with source from delayed stream
echo "Creating stream: ${PREFIX}-${SANITIZED}"
nats stream add "${PREFIX}-${SANITIZED}" \
  --subjects "${PREFIX}.endpoint.${SANITIZED}" \
  --retention work \
  --storage file \
  --replicas "${REPLICAS}" \
  --discard old \
  --max-msgs=-1 \
  --max-bytes=-1 \
  --max-age=0 \
  --duplicate-window=2m \
  --no-deny-delete \
  --no-deny-purge \
  --source "${PREFIX}-delayed" \
  --source-filter-subject "${PREFIX}.ready.${SANITIZED}"

# Create consumer (filters to unicast, ready, and event subjects)
echo "Creating consumer: ${SANITIZED}-main"
nats consumer add "${PREFIX}-${SANITIZED}" "${SANITIZED}-main" \
  --filter "${PREFIX}.endpoint.${SANITIZED},${PREFIX}.ready.${SANITIZED},${PREFIX}.events.>" \
  --ack explicit \
  --wait 30s \
  --max-deliver=-1 \
  --replay instant \
  --deliver all \
  --no-headers-only

echo "Done!"
```

Usage:
```bash
chmod +x create-endpoint.sh
./create-endpoint.sh nsb Orders 3
./create-endpoint.sh nsb Shipping 3
./create-endpoint.sh nsb Billing 3
```

## Monitoring

### Stream Information

```bash
# List all streams
nats stream list

# Stream details
nats stream info nsb-orders

# Stream statistics
nats stream report
```

### Consumer Information

```bash
# List consumers for a stream
nats consumer list nsb-orders

# Consumer details
nats consumer info nsb-orders orders-main

# Pending messages
nats consumer report nsb-orders
```

### Message Inspection

```bash
# View messages in a stream (non-destructive)
nats stream view nsb-orders --count 10

# View message by sequence number
nats stream get nsb-orders 42
```

## Troubleshooting

### Clear Stuck Messages

If messages are stuck (e.g., poison messages), you can purge them:

```bash
# Purge all messages from a stream
nats stream purge nsb-orders

# Purge messages matching a subject
nats stream purge nsb-orders --subject "nsb.endpoint.orders"

# Purge messages older than 1 hour
nats stream purge nsb-orders --keep 0 --seq 0
```

### Reset Consumer

To replay messages from the beginning:

```bash
# Delete and recreate the consumer
nats consumer delete nsb-orders orders-main

nats consumer add nsb-orders orders-main \
  --filter "nsb.endpoint.orders" \
  --ack explicit \
  --wait 30s \
  --max-deliver=-1 \
  --replay instant \
  --deliver all
```

### View Pending Acknowledgments

```bash
nats consumer info nsb-orders orders-main --json | jq '.num_ack_pending'
```

### Check JetStream Health

```bash
# Account info
nats account info

# Server info
nats server info

# JetStream report
nats server report jetstream
```

## High Availability

### Cluster Configuration

For production, run NATS in a cluster with 3+ nodes:

```conf
# nats-server.conf
server_name: nats-1
listen: 0.0.0.0:4222

jetstream {
    store_dir: /data/jetstream
}

cluster {
    name: nats-cluster
    listen: 0.0.0.0:6222
    routes: [
        nats-route://nats-1:6222
        nats-route://nats-2:6222
        nats-route://nats-3:6222
    ]
}
```

### Stream Replication

Set replicas when creating streams:

```bash
nats stream add nsb-orders \
  --subjects "nsb.endpoint.orders" \
  --replicas 3 \
  # ... other options
```

### Client Connection

Connect to multiple servers for failover:

```csharp
var transport = new NatsTransport("nats://nats-1:4222,nats://nats-2:4222,nats://nats-3:4222");
```

## Security

### Authentication

**Token-based:**
```bash
nats-server --auth mytoken
```

```csharp
var transport = new NatsTransport("nats://mytoken@server:4222");
```

**Username/password:**
```conf
authorization {
    users: [
        { user: app, password: secret, permissions: { ... } }
    ]
}
```

### Least-Privilege Permissions

For production, configure fine-grained permissions:

```conf
authorization {
    users: [
        {
            user: orders-endpoint
            password: $2a$11$...
            permissions: {
                # Publish to own endpoint and events
                publish: {
                    allow: [
                        "nsb.endpoint.>",
                        "nsb.events.>",
                        "nsb.delayed.>"
                    ]
                }
                # Subscribe to own streams
                subscribe: {
                    allow: [
                        "$JS.API.CONSUMER.MSG.NEXT.nsb-orders.>",
                        "$JS.API.CONSUMER.MSG.NEXT.nsb-events.>",
                        "$JS.API.STREAM.INFO.nsb-orders",
                        "$JS.API.STREAM.INFO.nsb-events",
                        "$JS.API.CONSUMER.INFO.nsb-orders.>",
                        "$JS.API.CONSUMER.INFO.nsb-events.>"
                    ]
                }
            }
        },
        {
            user: admin
            password: $2a$11$...
            permissions: {
                publish: ">"
                subscribe: ">"
            }
        }
    ]
}
```

### TLS

Enable TLS for encrypted connections:

```conf
tls {
    cert_file: "/etc/nats/server-cert.pem"
    key_file: "/etc/nats/server-key.pem"
    ca_file: "/etc/nats/ca.pem"
    verify: true
}
```

```csharp
var transport = new NatsTransport("nats://server:4222")
{
    ConfigureNatsOptions = opts => opts with
    {
        TlsOpts = new NatsTlsOpts
        {
            Mode = TlsMode.Require,
            CaFile = "/etc/nats/ca.pem"
        }
    }
};
```

## Backup and Recovery

### Export Stream Data

```bash
# Export stream to file
nats stream backup nsb-orders /backup/orders-$(date +%Y%m%d).tar.gz
```

### Restore Stream Data

```bash
# Restore from backup
nats stream restore nsb-orders /backup/orders-20240115.tar.gz
```

### Snapshots

For JetStream with file storage, you can also snapshot the storage directory directly while NATS is stopped.

## Capacity Planning

### Storage Estimates

- Message overhead: ~100 bytes per message (headers, metadata)
- Calculate: `(avg_message_size + 100) * messages_per_day * retention_days`

### Memory

- In-memory streams: size of all stored messages
- File-based streams: ~10% of stream size for indexes

### CPU

- Linear with message throughput
- Consider 1 core per 50,000 messages/second as baseline

## Next Steps

- [Configuration Guide](configuration.md) - Transport configuration options
- [Architecture](architecture.md) - Technical implementation details
