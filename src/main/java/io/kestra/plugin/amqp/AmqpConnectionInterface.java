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
        title = "Broker host",
        description = "Hostname or IP of the RabbitMQ broker; required unless using the deprecated `url`."
    )
    Property<String> getHost();

    @Schema(
        title = "Broker port",
        description = "TCP port for AMQP connections; defaults to `5672`."
    )
    Property<String> getPort();

    @Schema(
        title = "Virtual host",
        description = "Broker virtual host path; defaults to `/`."
    )
    Property<String> getVirtualHost();

    @Schema(
        title = "Username",
        description = "Username for the connection; uses broker default (often `guest`) when not set."
    )
    Property<String> getUsername();

    @Schema(
        title = "Password",
        description = "Password for the connection; required when the broker enforces authentication."
    )
    Property<String> getPassword();
}
