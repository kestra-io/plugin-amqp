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

        var factory = task.connectionFactory(runContextFactory.of());

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

        var factory = task.connectionFactory(runContextFactory.of());

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

        var factory = task.connectionFactory(runContextFactory.of());

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

        var factory = task.connectionFactory(runContextFactory.of());

        assertThat(factory.isSSL(), is(true));
    }

    @Test
    void shouldIgnoreCaCertificateWhenTlsIsOff() throws Exception {
        var task = DeclareExchange.builder()
            .host(Property.ofValue("localhost"))
            .sslCaCertificate(Property.ofValue(unitTestCa()))
            .name(Property.ofValue("tls.unit"))
            .build();

        var factory = task.connectionFactory(runContextFactory.of());

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

        var factory = task.connectionFactory(runContextFactory.of());

        assertThat(factory.isSSL(), is(true));
        assertThat(factory.getPort(), is(ConnectionFactory.DEFAULT_AMQP_OVER_SSL_PORT));
        assertThat(factory.getVirtualHost(), is("/my_vhost"));
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldLetExplicitSslFalseWinOverAmqpsUrl() throws Exception {
        var task = DeclareExchange.builder()
            .url(Property.ofValue("amqps://guest:guest@localhost/my_vhost"))
            .ssl(Property.ofValue(false))
            .name(Property.ofValue("tls.unit"))
            .build();

        var factory = task.connectionFactory(runContextFactory.of());

        assertThat(factory.isSSL(), is(false));
        assertThat(factory.getPort(), is(ConnectionFactory.DEFAULT_AMQP_PORT));
    }

    @Test
    void shouldRejectCaCertificateThatRendersEmpty() {
        var task = DeclareExchange.builder()
            .host(Property.ofValue("localhost"))
            .ssl(Property.ofValue(true))
            .sslCaCertificate(Property.ofValue("   "))
            .name(Property.ofValue("tls.unit"))
            .build();

        var error = assertThrows(IllegalArgumentException.class, () -> task.connectionFactory(runContextFactory.of()));
        assertThat(error.getMessage(), containsString("empty"));
    }

    @Test
    void shouldForwardTlsSettingsFromTrigger() throws Exception {
        var trigger = Trigger.builder()
            .id("tlsTrigger")
            .type(Trigger.class.getName())
            .host(Property.ofValue("localhost"))
            .ssl(Property.ofValue(true))
            .sslCaCertificate(Property.ofValue(unitTestCa()))
            .queue(Property.ofValue("tls.unit"))
            .maxRecords(Property.ofValue(1))
            .build();

        var factory = trigger.consumeTask().connectionFactory(runContextFactory.of());

        assertThat(factory.isSSL(), is(true));
        assertThat(factory.getPort(), is(ConnectionFactory.DEFAULT_AMQP_OVER_SSL_PORT));
    }

    @Test
    void shouldForwardTlsSettingsFromRealtimeTrigger() throws Exception {
        var trigger = RealtimeTrigger.builder()
            .id("tlsRealtimeTrigger")
            .type(RealtimeTrigger.class.getName())
            .host(Property.ofValue("localhost"))
            .ssl(Property.ofValue(true))
            .sslCaCertificate(Property.ofValue("not a certificate"))
            .queue(Property.ofValue("tls.unit"))
            .build();

        var task = trigger.consumeTask();

        // The invalid CA only fails if it reached the Consume task.
        var error = assertThrows(IllegalArgumentException.class, () -> task.connectionFactory(runContextFactory.of()));
        assertThat(error.getMessage(), containsString("sslCaCertificate"));
    }
}
