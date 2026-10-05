package io.kestra.plugin.amqp;

import java.time.Duration;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

public interface Amqp1ConnectionInterface {
    int DEFAULT_PORT = 5672;
    int DEFAULT_SSL_PORT = 5671;
    Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(30);

    @NotNull
    @Schema(
        title = "Broker host",
        description = "Hostname or IP of the AMQP 1.0 broker, e.g. `my-namespace.servicebus.windows.net` for Azure Service Bus."
    )
    @PluginProperty(group = "connection")
    Property<String> getHost();

    @Schema(
        title = "Broker port",
        description = "TCP port of the AMQP 1.0 listener; defaults to `5672`, or `5671` when `ssl` is enabled."
    )
    @PluginProperty(group = "connection")
    Property<Integer> getPort();

    @Schema(
        title = "Username",
        description = "SASL PLAIN username; for Azure Service Bus use the shared access policy name (e.g. `RootManageSharedAccessKey`). SASL ANONYMOUS is used when not set."
    )
    @PluginProperty(secret = true, group = "connection")
    Property<String> getUsername();

    @Schema(
        title = "Password",
        description = "SASL PLAIN password; for Azure Service Bus use the shared access key."
    )
    @PluginProperty(secret = true, group = "connection")
    Property<String> getPassword();

    @Schema(
        title = "Use TLS (AMQPS)",
        description = """
            Encrypts the connection with TLS. The broker certificate and hostname are always verified, \
            against the JVM's trusted certificate authorities unless `sslCaCertificate` is set. \
            Required by Azure Service Bus. Defaults to `false`."""
    )
    @PluginProperty(group = "connection")
    Property<Boolean> getSsl();

    @Schema(
        title = "CA certificate for TLS",
        description = """
            PEM-encoded certificate (or chain) of the authority that signed the broker certificate, \
            for brokers using a private or self-signed CA. Only used when `ssl` is `true`; \
            when set, only this CA is trusted. Example: `{{ secret('AMQP_CA_CERT') }}`."""
    )
    @PluginProperty(group = "connection")
    Property<String> getSslCaCertificate();

    @Schema(
        title = "Connection timeout",
        description = "Maximum time to open or close the connection and its links, and to wait for the broker to settle each published message. Defaults to `PT30S`."
    )
    @PluginProperty(group = "reliability")
    Property<Duration> getConnectionTimeout();
}
