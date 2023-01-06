package io.kestra.plugin.amqp;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.NotNull;

public interface AmqpConnectionInterface {
    @NotNull
    @PluginProperty(dynamic = true)
    @Schema(
        title = "The connection string"
    )
    String getUri();
}
