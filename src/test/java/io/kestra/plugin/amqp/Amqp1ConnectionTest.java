package io.kestra.plugin.amqp;

import java.time.Duration;

import org.apache.qpid.protonj2.client.Client;
import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class Amqp1ConnectionTest extends AbstractAmqp1Test {
    @Test
    void shouldUseDefaults() throws Exception {
        var task = Amqp1Publish.builder().host(Property.ofValue(HOST)).build();
        var runContext = this.runContextFactory.of();

        var options = task.connectionOptions(runContext);

        assertThat(options.sslEnabled(), is(false));
        assertThat(options.user(), is(nullValue()));
        assertThat(options.password(), is(nullValue()));
        assertThat(options.openTimeout(), is(Amqp1ConnectionInterface.DEFAULT_CONNECTION_TIMEOUT.toMillis()));
        assertThat(options.sendTimeout(), is(Amqp1ConnectionInterface.DEFAULT_CONNECTION_TIMEOUT.toMillis()));
        assertThat(task.port(runContext, options.sslEnabled()), is(Amqp1ConnectionInterface.DEFAULT_PORT));
    }

    @Test
    void shouldConfigureSslCredentialsAndTimeouts() throws Exception {
        var task = Amqp1Publish.builder()
            .host(Property.ofValue("my-namespace.servicebus.windows.net"))
            .ssl(Property.ofValue(true))
            .username(Property.ofValue("RootManageSharedAccessKey"))
            .password(Property.ofValue("sas-key"))
            .connectionTimeout(Property.ofValue(Duration.ofSeconds(5)))
            .build();
        var runContext = this.runContextFactory.of();

        var options = task.connectionOptions(runContext);

        assertThat(options.sslEnabled(), is(true));
        assertThat(options.user(), is("RootManageSharedAccessKey"));
        assertThat(options.password(), is("sas-key"));
        assertThat(options.openTimeout(), is(5000L));
        assertThat(options.closeTimeout(), is(5000L));
        assertThat(options.requestTimeout(), is(5000L));
        assertThat(task.port(runContext, options.sslEnabled()), is(Amqp1ConnectionInterface.DEFAULT_SSL_PORT));
    }

    @Test
    void shouldPreferAnExplicitPort() throws Exception {
        var task = Amqp1Publish.builder().host(Property.ofValue(HOST)).port(Property.ofValue(12345)).ssl(Property.ofValue(true)).build();

        assertThat(task.port(this.runContextFactory.of(), true), is(12345));
    }

    @Test
    void shouldFailWithAnActionableMessageOnInvalidCredentials() {
        var task = this.publishTask(randomAddress()).password(Property.ofValue("wrong-password")).build();

        var exception = assertThrows(IllegalStateException.class, () ->
        {
            try (var client = Client.create()) {
                task.connect(this.runContextFactory.of(), client);
            }
        });
        assertThat(exception.getMessage(), allOf(containsString("Unable to connect"), containsString(HOST + ":" + PORT)));
    }
}
