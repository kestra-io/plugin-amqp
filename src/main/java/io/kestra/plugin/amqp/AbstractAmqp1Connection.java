package io.kestra.plugin.amqp;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientException;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;

import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractAmqp1Connection extends Task implements Amqp1ConnectionInterface {
    @NotNull
    private Property<String> host;

    private Property<Integer> port;

    private Property<String> username;

    @PluginProperty(secret = true, group = "connection")
    @ToString.Exclude
    private Property<String> password;

    @Builder.Default
    private Property<Boolean> ssl = Property.ofValue(false);

    @Builder.Default
    private Property<Duration> connectionTimeout = Property.ofValue(DEFAULT_CONNECTION_TIMEOUT);

    /**
     * Opens a connection owned by {@code client} (closing the client closes it) and waits until the broker
     * accepted it, so that unreachable hosts, TLS or SASL failures surface here with an actionable message.
     */
    protected Connection connect(RunContext runContext, Client client) throws IllegalVariableEvaluationException, ClientException, InterruptedException {
        var rHost = runContext.render(this.host).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("`host` is required to connect to the AMQP 1.0 broker"));
        var options = this.connectionOptions(runContext);
        var rPort = this.port(runContext, options.sslEnabled());

        runContext.logger().debug("Connecting to AMQP 1.0 broker {}:{} (ssl: {})", rHost, rPort, options.sslEnabled());
        var connection = client.connect(rHost, rPort, options);
        try {
            connection.openFuture().get(options.openTimeout(), TimeUnit.MILLISECONDS);
            return connection;
        } catch (ExecutionException e) {
            connection.closeAsync();
            throw new IllegalStateException(
                "Unable to connect to the AMQP 1.0 broker at " + rHost + ":" + rPort + ": " + e.getCause().getMessage() + ". Check host, port, ssl and credentials.",
                e.getCause()
            );
        } catch (TimeoutException e) {
            connection.closeAsync();
            throw new IllegalStateException(
                "Timed out after " + options.openTimeout() + " ms connecting to the AMQP 1.0 broker at " + rHost + ":" + rPort + ". Check host, port and ssl, or increase `connectionTimeout`.",
                e
            );
        }
    }

    ConnectionOptions connectionOptions(RunContext runContext) throws IllegalVariableEvaluationException {
        var rTimeout = runContext.render(this.connectionTimeout).as(Duration.class).orElse(DEFAULT_CONNECTION_TIMEOUT).toMillis();

        var options = new ConnectionOptions()
            .sslEnabled(runContext.render(this.ssl).as(Boolean.class).orElse(false))
            .openTimeout(rTimeout)
            .closeTimeout(rTimeout)
            .sendTimeout(rTimeout)
            .requestTimeout(rTimeout);
        runContext.render(this.username).as(String.class).ifPresent(options::user);
        runContext.render(this.password).as(String.class).ifPresent(options::password);

        return options;
    }

    int port(RunContext runContext, boolean ssl) throws IllegalVariableEvaluationException {
        return runContext.render(this.port).as(Integer.class).orElse(ssl ? DEFAULT_SSL_PORT : DEFAULT_PORT);
    }
}
