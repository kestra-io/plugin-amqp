package io.kestra.plugin.amqp;

import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Duration;

public interface ConsumeInterface extends ConsumeBaseInterface {
    @Schema(
        title = "The maximum number of rows to fetch before stopping.",
        description = "It's not an hard limit and is evaluated every second."
    )
    Integer getMaxRecords();

    @Schema(
        title = "The maximum duration to wait for new rows.",
        description = "It's not an hard limit and is evaluated every second."
    )
    Duration getMaxDuration();
}
