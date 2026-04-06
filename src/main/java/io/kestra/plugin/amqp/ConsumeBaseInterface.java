package io.kestra.plugin.amqp;

import io.kestra.core.models.property.Property;
import io.kestra.plugin.amqp.models.SerdeType;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import io.kestra.core.models.annotations.PluginProperty;

public interface ConsumeBaseInterface {
    @NotNull
    @Schema(
        title = "Queue name to consume",
        description = "AMQP queue to read from; required and must already exist."
    )
    @PluginProperty(group = "main")
    Property<String> getQueue();

    @Schema(
        title = "Consumer tag",
        description = "Client-supplied consumer tag used for tracing and cancellations; defaults to `Kestra` in tasks and triggers."
    )
    @NotNull
    @PluginProperty(group = "main")
    Property<String> getConsumerTag();

    @Schema(
        title = "Payload serde format",
        description = "Controls how message bodies are read and written; use STRING for raw text or JSON for structured data. Defaults to STRING."
    )
    @NotNull
    @PluginProperty(group = "main")
    Property<SerdeType> getSerdeType();
}
