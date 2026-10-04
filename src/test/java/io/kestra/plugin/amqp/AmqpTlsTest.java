package io.kestra.plugin.amqp;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.junit.jupiter.api.Test;

import com.rabbitmq.client.ConnectionFactory;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Checks how the connection factory is configured for TLS. No broker needed.
 */
@KestraTest
class AmqpTlsTest {
    @Inject
    private RunContextFactory runContextFactory;

    private static String unitTestCa() throws Exception {
        try (var in = Objects.requireNonNull(AmqpTlsTest.class.getResourceAsStream("/tls/unit-test-ca.pem"))) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @Test
    void shouldKeepPlainAmqpByDefault() throws Exception {
        var task = DeclareExchange.builder()
            .host(Property.ofValue("localhost"))
            .name(Property.ofValue("tls.unit"))
            .build();

        ConnectionFactory factory = task.connectionFactory(runContextFactory.of());

        assertThat(factory.isSSL(), is(false));
        assertThat(factory.getPort(), is(ConnectionFactory.DEFAULT_AMQP_PORT));
    }

    @Test
    void shouldUseTlsAndDefaultAmqpsPort() throws Exception {
        var task = DeclareExchange.builder()
            .host(Property.ofValue("localhost"))
            .ssl(Property.ofValue(true))
            .name(Property.ofValue("tls.unit"))
            .build();

        ConnectionFactory factory = task.connectionFactory(runContextFactory.of());

        assertThat(factory.isSSL(), is(true));
        assertThat(factory.getPort(), is(ConnectionFactory.DEFAULT_AMQP_OVER_SSL_PORT));
    }

    @Test
    void shouldKeepExplicitPortWithTls() throws Exception {
        var task = DeclareExchange.builder()
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("15671"))
            .ssl(Property.ofValue(true))
            .name(Property.ofValue("tls.unit"))
            .build();

        ConnectionFactory factory = task.connectionFactory(runContextFactory.of());

        assertThat(factory.isSSL(), is(true));
        assertThat(factory.getPort(), is(15671));
    }

    @Test
    void shouldTrustProvidedCaCertificate() throws Exception {
        var task = DeclareExchange.builder()
            .host(Property.ofValue("localhost"))
            .ssl(Property.ofValue(true))
            .sslCaCertificate(Property.ofValue(unitTestCa()))
            .name(Property.ofValue("tls.unit"))
            .build();

        ConnectionFactory factory = task.connectionFactory(runContextFactory.of());

        assertThat(factory.isSSL(), is(true));
    }

    @Test
    void shouldIgnoreCaCertificateWhenTlsIsOff() throws Exception {
        var task = DeclareExchange.builder()
            .host(Property.ofValue("localhost"))
            .sslCaCertificate(Property.ofValue(unitTestCa()))
            .name(Property.ofValue("tls.unit"))
            .build();

        ConnectionFactory factory = task.connectionFactory(runContextFactory.of());

        assertThat(factory.isSSL(), is(false));
        assertThat(factory.getPort(), is(ConnectionFactory.DEFAULT_AMQP_PORT));
    }

    @Test
    void shouldRejectInvalidCaCertificate() {
        var task = DeclareExchange.builder()
            .host(Property.ofValue("localhost"))
            .ssl(Property.ofValue(true))
            .sslCaCertificate(Property.ofValue("not a certificate"))
            .name(Property.ofValue("tls.unit"))
            .build();

        var error = assertThrows(IllegalArgumentException.class, () -> task.connectionFactory(runContextFactory.of()));
        assertThat(error.getMessage(), containsString("sslCaCertificate"));
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldEnableTlsForAmqpsUrl() throws Exception {
        var task = DeclareExchange.builder()
            .url(Property.ofValue("amqps://guest:guest@localhost/my_vhost"))
            .name(Property.ofValue("tls.unit"))
            .build();

        ConnectionFactory factory = task.connectionFactory(runContextFactory.of());

        assertThat(factory.isSSL(), is(true));
        assertThat(factory.getPort(), is(ConnectionFactory.DEFAULT_AMQP_OVER_SSL_PORT));
        assertThat(factory.getVirtualHost(), is("/my_vhost"));
    }
}
