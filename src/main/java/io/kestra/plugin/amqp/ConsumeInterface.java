package io.kestra.plugin.amqp;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Duration;

public interface ConsumeInterface extends ConsumeBaseInterface {
    @Schema(
        title = "Maximum number of records",
        description = "The maximum number of messages to consume before stopping. " +
            "This is a soft limit evaluated after each message."
    )
    Property<Integer> getMaxRecords();

    @Schema(
        title = "Maximum duration",
        description = "The maximum duration the consumer will run before stopping. " +
            "This is a soft limit evaluated approximately every 100 milliseconds, " +
            "so the actual duration may slightly exceed this value."
    )
    Property<Duration> getMaxDuration();
}
