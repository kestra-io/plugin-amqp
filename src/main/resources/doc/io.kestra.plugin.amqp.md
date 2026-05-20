# How to use the AMQP plugin

Publish and consume messages on AMQP brokers (RabbitMQ and compatible) from Kestra flows.

## Common properties

Set `host`, `port` (default `5672`), `username`, `password`, and `virtualHost` (default `/`) on each task. Store credentials in [secrets](https://kestra.io/docs/concepts/secret) and apply them globally with [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults).

## Tasks

`Publish` sends messages to an exchange — set `exchange`, optionally `routingKey`, and pass messages via `from`. Use `serdeType: JSON` to serialize objects as JSON; the default is `STRING`.

`Consume` reads messages from a `queue`. Set at least one of `maxRecords` or `maxDuration` to bound the batch. Set `autoAck: true` to acknowledge automatically, or leave it `false` to acknowledge after processing.

`DeclareExchange` creates an exchange — set `name` and `exchangeType` (`DIRECT`, `FANOUT`, `TOPIC`, or `HEADERS`). `CreateQueue` declares a queue by `name`. `QueueBind` binds a `queue` to an `exchange` with an optional `routingKey`. Use these setup tasks in flows that provision broker topology before publishing.

`Trigger` polls a queue on a schedule (default 60 seconds) and starts one execution per batch. `RealtimeTrigger` starts one execution per message as it arrives. Use `Trigger` for controlled throughput and `RealtimeTrigger` for low-latency processing.
