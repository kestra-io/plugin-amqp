package io.kestra.plugin.amqp;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.plugin.amqp.models.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

public interface ConsumeBaseInterface {
    @NotNull
    @Schema(
        title = "The queue to pull messages from"
    )
    Property<String> getQueue();

    @Schema(
        title = "A client-generated consumer tag to establish context"
    )
    @NotNull
    Property<String> getConsumerTag();

    @NotNull
    Property<SerdeType> getSerdeType();
}
