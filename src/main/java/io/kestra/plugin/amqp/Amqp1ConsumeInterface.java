package io.kestra.plugin.amqp;

import java.time.Duration;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.plugin.amqp.models.SerdeType;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

public interface Amqp1ConsumeInterface {
    Duration DEFAULT_POLL_DURATION = Duration.ofSeconds(5);

    @NotNull
    @Schema(
        title = "Source address",
        description = """
            Address to consume from, in the broker's format: the queue name for ActiveMQ Artemis and Azure Service Bus \
            (`<topic>/Subscriptions/<subscription>` for a Service Bus topic subscription), or `/queues/<queue>` for RabbitMQ 4."""
    )
    @PluginProperty(group = "main")
    Property<String> getAddress();

    @NotNull
    @Schema(
        title = "Payload serde format",
        description = "Controls how message bodies are read; use STRING for raw text or JSON for structured data. Defaults to STRING."
    )
    @PluginProperty(group = "main")
    Property<SerdeType> getSerdeType();

    @Schema(
        title = "Automatic acknowledgment",
        description = """
            When true, each message is accepted as soon as it is received; it is not redelivered if the task is killed mid-batch.
            When false, every message of the batch is accepted once the batch is durably stored, and released back to the broker
            if the task fails or is killed before that point. On Azure Service Bus (peek-lock), keep the batch shorter than the
            queue's lock duration. Defaults to `false`."""
    )
    @PluginProperty(group = "reliability")
    Property<Boolean> getAutoAck();

    @Schema(
        title = "Maximum records",
        description = "Number of messages after which consumption stops; credit is never granted beyond it, so the broker keeps the remaining messages. Required when `maxDuration` is not set."
    )
    @PluginProperty(group = "execution")
    Property<Integer> getMaxRecords();

    @Schema(
        title = "Maximum duration",
        description = "Time after which consumption stops. Required when `maxRecords` is not set."
    )
    @PluginProperty(group = "execution")
    Property<Duration> getMaxDuration();

    @NotNull
    @Schema(
        title = "Poll duration",
        description = "Maximum time to wait for the next message; consumption stops early when none arrives within it, e.g. once the queue is drained. Defaults to `PT5S`."
    )
    @PluginProperty(group = "execution")
    Property<Duration> getPollDuration();
}
