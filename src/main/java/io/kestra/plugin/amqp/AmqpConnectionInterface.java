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
        description = "TCP port for AMQP connections; defaults to `5672`, or `5671` when `ssl` is `true`."
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
    @PluginProperty(secret = true, group = "connection")
    Property<String> getUsername();

    @Schema(
        title = "Password",
        description = "Password for the connection; required when the broker enforces authentication."
    )
    @PluginProperty(secret = true, group = "connection")
    Property<String> getPassword();

    @Schema(
        title = "Use TLS (AMQPS)",
        description = "Connect to the broker over TLS. The broker certificate and hostname are always verified, " +
            "against the JVM's trusted certificate authorities unless `sslCaCertificate` is set. " +
            "The port defaults to `5671` when TLS is enabled. Defaults to `false`."
    )
    @PluginProperty(group = "connection")
    Property<Boolean> getSsl();

    @Schema(
        title = "CA certificate for TLS",
        description = "PEM-encoded certificate (or chain) of the authority that signed the broker certificate, " +
            "for brokers using a private or self-signed CA. Only used when `ssl` is `true`; " +
            "when set, only this CA is trusted. Example: `{{ secret('RABBITMQ_CA_CERT') }}`."
    )
    @PluginProperty(group = "connection")
    Property<String> getSslCaCertificate();
}
