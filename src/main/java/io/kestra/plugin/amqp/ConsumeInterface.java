package io.kestra.plugin.amqp;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Duration;

public interface ConsumeInterface extends ConsumeBaseInterface {

    @Schema(
        title = "Maximum records",
        description = "Soft cap on messages consumed before stopping; evaluated after each ACKed message. Required when `maxDuration` is not set."
    )
    Property<Integer> getMaxRecords();

    @Schema(
        title = "Maximum duration",
        description = "Soft cap on run time; checked roughly every 100 ms so actual runtime can slightly exceed this value. Required when `maxRecords` is not set."
    )
    Property<Duration> getMaxDuration();
}
