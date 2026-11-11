---
nats-transport: patch
---

Update NServiceBus, NATS.Net, and Microsoft dependencies. Validate delayed delivery against NATS 2.15.2, prevent JetStream control headers from cancelling retries, and keep concurrently published event types on one source cursor.
