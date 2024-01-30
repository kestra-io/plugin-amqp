package io.kestra.plugin.amqp;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

public interface AmqpConnectionInterface {

    @Deprecated
    @Schema(hidden = true)
    String getUrl();

    @PluginProperty(dynamic = true)
    @Schema(
        title = "The broker host."
    )
    String getHost();

    @PluginProperty(dynamic = true)
    @Schema(
        title = "The broker port."
    )
    String getPort();

    @PluginProperty(dynamic = true)
    @Schema(
        title = "The broker virtual host."
    )
    String getVirtualHost();

    @PluginProperty(dynamic = true)
    @Schema(
        title = "The broker username."
    )
    String getUsername();

    @PluginProperty(dynamic = true)
    @Schema(
        title = "The broker password."
    )
    String getPassword();
}
