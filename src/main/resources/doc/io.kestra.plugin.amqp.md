# How to use the AMQP plugin

Publish and consume messages on AMQP brokers from Kestra flows: AMQP 0.9.1 (RabbitMQ and compatible) and AMQP 1.0 (Azure Service Bus, ActiveMQ Artemis, RabbitMQ 4, and other compliant brokers) with the `Amqp1*` tasks.

## Common properties

Set `host`, `port` (default `5672`), `username`, `password`, and `virtualHost` (default `/`) on each task. Store credentials in [secrets](https://kestra.io/docs/concepts/secret) and set them on each task.

To connect over TLS (AMQPS), set `ssl: true`. The port then defaults to `5671`, and the broker certificate and hostname are always verified against the JVM's trusted certificate authorities. For a broker signed by a private or self-signed CA, set `sslCaCertificate` to the PEM certificate (or chain) of that CA, stored in a secret, for example `sslCaCertificate: "{{ secret('RABBITMQ_CA_CERT') }}"`; only that CA is then trusted. With the deprecated `url`, an `amqps://` scheme enables TLS unless `ssl` is set explicitly.

## Tasks

`Publish` sends messages to an exchange — set `exchange`, optionally `routingKey`, and pass messages via `from`. Use `serdeType: JSON` to serialize objects as JSON; the default is `STRING`.

`Consume` reads messages from a `queue`. Set at least one of `maxRecords` or `maxDuration` to bound the batch. Set `autoAck: true` to acknowledge each message on delivery, or leave it `false` to send a single bulk acknowledgment for the whole batch once it is durably stored — if the task is killed before that point, the broker requeues every unacknowledged message; `autoAck: true` messages are not requeued on kill.

`DeclareExchange` creates an exchange — set `name` and `exchangeType` (`DIRECT`, `FANOUT`, `TOPIC`, or `HEADERS`). `CreateQueue` declares a queue by `name`. `QueueBind` binds a `queue` to an `exchange` with an optional `routingKey`. Use these setup tasks in flows that provision broker topology before publishing.

`Trigger` polls a queue on a schedule (default 60 seconds) and starts one execution per batch, sharing `Consume`'s ack behavior and kill semantics — a killed poll never emits an execution for the batch it was consuming. `RealtimeTrigger` starts one execution per message as it arrives. Use `Trigger` for controlled throughput and `RealtimeTrigger` for low-latency processing.

## AMQP 1.0

The `Amqp1Publish`, `Amqp1Consume`, and `Amqp1Trigger` tasks speak AMQP 1.0 and set `host`, `port` (default `5672`, or `5671` when `ssl: true`), `username`, `password`, and `ssl` on each task. For Azure Service Bus, set `ssl: true`, the shared access policy name (e.g. `RootManageSharedAccessKey`) as `username` and its key as `password`. Instead of exchanges and routing keys, messages go to an `address`: the queue or topic name on Azure Service Bus and ActiveMQ Artemis (`<topic>/Subscriptions/<subscription>` to consume a Service Bus subscription), or `/queues/<queue>` on RabbitMQ 4.

`Amqp1Publish` sends messages from `from`; each message can carry `applicationProperties` (custom properties), `messageAnnotations`, and the standard AMQP 1.0 properties such as `messageId`, `subject`, `groupId` (Service Bus session ID), `durable`, or `timeToLive`.

`Amqp1Consume` reads messages from an `address` until `maxRecords` or `maxDuration` is reached, or no message arrives within `pollDuration` (default 5 seconds). Messages are accepted once the batch is durably stored and released back to the broker if the task fails or is killed; set `autoAck: true` to accept each message on receipt instead. `Amqp1Trigger` polls an address on a schedule (default 60 seconds) and starts one execution per batch with the same semantics.
