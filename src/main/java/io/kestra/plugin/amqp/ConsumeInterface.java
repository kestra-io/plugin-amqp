package io.kestra.plugin.amqp;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.plugin.amqp.services.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.NotNull;
import java.time.Duration;

public interface ConsumeInterface {
    @NotNull
    @PluginProperty(dynamic = true)
    @Schema(
        title = "The queue to pull messages from."
    )
    String getQueue();

    @Schema(
        title = "A client-generated consumer tag to establish context."
    )
    @NotNull
    String getConsumerTag();

    @Schema(
        title = "The max number of rows to fetch before stopping.",
        description = "It's not an hard limit and is evaluated every second."
    )
    Integer getMaxRecords();

    @Schema(
        title = "The max duration waiting for new rows.",
        description = "It's not an hard limit and is evaluated every second."
    )
    Duration getMaxDuration();

    @NotNull
    SerdeType getSerdeType();
}
