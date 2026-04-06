package io.kestra.plugin.amqp;

import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;
import io.kestra.core.models.annotations.PluginProperty;

public interface AmqpConnectionInterface {

    @Deprecated
    @Schema(hidden = true)
    @PluginProperty(group = "connection")
    Property<String> getUrl();

    @Schema(
        title = "Broker host",
        description = "Hostname or IP of the RabbitMQ broker; required unless using the deprecated `url`."
    )
    @PluginProperty(group = "connection")
    Property<String> getHost();

    @Schema(
        title = "Broker port",
        description = "TCP port for AMQP connections; defaults to `5672`."
    )
    @PluginProperty(group = "connection")
    Property<String> getPort();

    @Schema(
        title = "Virtual host",
        description = "Broker virtual host path; defaults to `/`."
    )
    @PluginProperty(group = "connection")
    Property<String> getVirtualHost();

    @Schema(
        title = "Username",
        description = "Username for the connection; uses broker default (often `guest`) when not set."
    )
    @PluginProperty(group = "connection")
    Property<String> getUsername();

    @Schema(
        title = "Password",
        description = "Password for the connection; required when the broker enforces authentication."
    )
    @PluginProperty(group = "connection")
    Property<String> getPassword();
}
