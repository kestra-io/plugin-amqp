package io.kestra.plugin.amqp;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.plugin.amqp.models.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

public interface ConsumeBaseInterface {
    @NotNull
    @PluginProperty(dynamic = true)
    @Schema(
        title = "The queue to pull messages from."
    )
    String getQueue();

    @PluginProperty(dynamic = true)
    @Schema(
        title = "A client-generated consumer tag to establish context."
    )
    @NotNull
    String getConsumerTag();

    @NotNull
    SerdeType getSerdeType();
}
