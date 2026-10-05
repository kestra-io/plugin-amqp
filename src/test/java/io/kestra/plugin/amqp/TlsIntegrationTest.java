package io.kestra.plugin.amqp;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.amqp.models.SerdeType;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TlsIntegrationTest {
    @Inject
    private RunContextFactory runContextFactory;

    private String ca;

    @BeforeAll
    void loadCa() throws Exception {
        try (var in = TlsIntegrationTest.class.getResourceAsStream("/tls/ca.crt")) {
            assumeTrue(in != null, "Run .github/setup-unit.sh first to start the TLS broker");
            ca = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @Test
    void shouldPublishAndConsumeOverTls() throws Exception {
        var suffix = IdUtils.create();
        var exchange = "amqpTls.exchange." + suffix;
        var queue = "amqpTls.queue." + suffix;

        DeclareExchange.builder()
            .host(Property.ofValue("localhost"))
            .ssl(Property.ofValue(true))
            .sslCaCertificate(Property.ofValue(ca))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .name(Property.ofValue(exchange))
            .build()
            .run(runContextFactory.of());

        CreateQueue.builder()
            .host(Property.ofValue("localhost"))
            .ssl(Property.ofValue(true))
            .sslCaCertificate(Property.ofValue(ca))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .name(Property.ofValue(queue))
            .build()
            .run(runContextFactory.of());

        QueueBind.builder()
            .host(Property.ofValue("localhost"))
            .ssl(Property.ofValue(true))
            .sslCaCertificate(Property.ofValue(ca))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .exchange(Property.ofValue(exchange))
            .queue(Property.ofValue(queue))
            .build()
            .run(runContextFactory.of());

        var published = Publish.builder()
            .host(Property.ofValue("localhost"))
            .ssl(Property.ofValue(true))
            .sslCaCertificate(Property.ofValue(ca))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .exchange(Property.ofValue(exchange))
            .from(List.of(Map.of("data", "over-tls")))
            .build()
            .run(runContextFactory.of());
        assertThat(published.getMessagesCount(), is(1));

        var consumed = Consume.builder()
            .id("tlsConsume-" + suffix)
            .type(Consume.class.getName())
            .host(Property.ofValue("localhost"))
            .ssl(Property.ofValue(true))
            .sslCaCertificate(Property.ofValue(ca))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .queue(Property.ofValue(queue))
            .serdeType(Property.ofValue(SerdeType.STRING))
            .maxRecords(Property.ofValue(1))
            .maxDuration(Property.ofValue(Duration.ofSeconds(10)))
            .build()
            .run(runContextFactory.of());
        assertThat(consumed.getCount(), is(1));
    }

    @Test
    void shouldRejectBrokerSignedByUnknownCa() {
        var task = DeclareExchange.builder()
            .host(Property.ofValue("localhost"))
            .ssl(Property.ofValue(true))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .name(Property.ofValue("amqpTls.untrusted." + IdUtils.create()))
            .build();

        // The JVM trust store does not know the test CA.
        var error = assertThrows(Exception.class, () -> task.run(runContextFactory.of()));
        assertThat(hasCause(error, SSLException.class), is(true));
    }

    private static boolean hasCause(Throwable error, Class<? extends Throwable> type) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (type.isInstance(t)) {
                return true;
            }
        }
        return false;
    }
}
