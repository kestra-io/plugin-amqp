package io.kestra.plugin.amqp;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

public interface AmqpConnectionInterface {

    @Deprecated
    @Schema(hidden = true)
    Property<String> getUrl();

    @Schema(
        title = "The broker host."
    )
    Property<String> getHost();

    @Schema(
        title = "The broker port."
    )
    Property<String> getPort();

    @Schema(
        title = "The broker virtual host."
    )
    Property<String> getVirtualHost();

    @Schema(
        title = "The broker username."
    )
    Property<String> getUsername();

    @Schema(
        title = "The broker password."
    )
    Property<String> getPassword();
}
