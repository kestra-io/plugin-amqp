# Kestra AMQP Plugin

## What

- Provides plugin components under `io.kestra.plugin.amqp`.
- Includes classes such as `Consume`, `QueueBind`, `AmqpExceptionHandler`, `Trigger`.

## Why

- This plugin integrates Kestra with AMQP.
- It provides tasks that connect to RabbitMQ brokers to declare exchanges and queues, bind routing keys, publish messages, and consume them via tasks or triggers (batch or real time).

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
