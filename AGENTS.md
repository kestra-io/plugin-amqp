# Kestra AMQP Plugin

## What

- Provides plugin components under `io.kestra.plugin.amqp`.
- Includes classes such as `Consume`, `QueueBind`, `AmqpExceptionHandler`, `Trigger`.

## Why

- What user problem does this solve? Teams need to connect to RabbitMQ brokers to declare exchanges and queues, bind routing keys, publish messages, and consume them via tasks or triggers (batch or real time) from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps AMQP steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on AMQP.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `amqp`

Infrastructure dependencies (Docker Compose services):

- `rabbitmq`

### Key Plugin Classes

- `io.kestra.plugin.amqp.Consume`
- `io.kestra.plugin.amqp.CreateQueue`
- `io.kestra.plugin.amqp.DeclareExchange`
- `io.kestra.plugin.amqp.Publish`
- `io.kestra.plugin.amqp.QueueBind`
- `io.kestra.plugin.amqp.RealtimeTrigger`
- `io.kestra.plugin.amqp.Trigger`

### Project Structure

```
plugin-amqp/
├── src/main/java/io/kestra/plugin/amqp/models/
├── src/test/java/io/kestra/plugin/amqp/models/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
