package io.kestra.plugin.amqp;

import com.fasterxml.jackson.core.JsonParseException;
import com.google.common.collect.ImmutableMap;
import com.rabbitmq.client.ConnectionFactory;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.amqp.models.Message;
import io.kestra.plugin.amqp.models.SerdeType;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AMQPTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    protected StorageInterface storageInterface;

    @Test
    void createConnectionFactoryWithDefaultValues() throws Exception {
        Publish push = Publish.builder().build();

        ConnectionFactory connectionFactory = push.connectionFactory(runContextFactory.of());
        assertThat(connectionFactory.getHost(), is(ConnectionFactory.DEFAULT_HOST));
        assertThat(connectionFactory.getPort(), is(ConnectionFactory.DEFAULT_AMQP_PORT));
        assertThat(connectionFactory.getUsername(), is(ConnectionFactory.DEFAULT_USER));
        assertThat(connectionFactory.getPassword(), is(ConnectionFactory.DEFAULT_PASS));
        assertThat(connectionFactory.getVirtualHost(), is(ConnectionFactory.DEFAULT_VHOST));
    }

    @Test
    void createConnectionFactoryWithUriOnly() throws Exception {
        Publish push = Publish.builder()
            .url(Property.ofValue("amqp://kestra:K3str4@example.org:12345/my_vhost"))
            .build();

        ConnectionFactory connectionFactory = push.connectionFactory(runContextFactory.of());
        assertThat(connectionFactory.getHost(), is("example.org"));
        assertThat(connectionFactory.getPort(), is(12345));
        assertThat(connectionFactory.getUsername(), is("kestra"));
        assertThat(connectionFactory.getPassword(), is("K3str4"));
        assertThat(connectionFactory.getVirtualHost(), is("/my_vhost"));
    }

    @Test
    void createConnectionFactoryWithFieldsOnly() throws Exception {
        Publish push = Publish.builder()
            .host(Property.ofValue("example.org"))
            .port(Property.ofValue("12345"))
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("K3str4"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .build();

        ConnectionFactory connectionFactory = push.connectionFactory(runContextFactory.of());
        assertThat(connectionFactory.getHost(), is("example.org"));
        assertThat(connectionFactory.getPort(), is(12345));
        assertThat(connectionFactory.getUsername(), is("kestra"));
        assertThat(connectionFactory.getPassword(), is("K3str4"));
        assertThat(connectionFactory.getVirtualHost(), is("/my_vhost"));
    }

    @Test
    void createConnectionFactoryWithUriAndFieldsButNoHost() throws Exception {
        Publish push = Publish.builder()
            .url(Property.ofValue("amqp://example.org"))
            .port(Property.ofValue("12345"))
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("K3str4"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .build();

        ConnectionFactory connectionFactory = push.connectionFactory(runContextFactory.of());
        assertThat(connectionFactory.getHost(), is("example.org"));
        assertThat(connectionFactory.getPort(), is(12345));
        assertThat(connectionFactory.getUsername(), is("kestra"));
        assertThat(connectionFactory.getPassword(), is("K3str4"));
        assertThat(connectionFactory.getVirtualHost(), is("/my_vhost"));
    }

    @Test
    void createConnectionFactoryWithUriAndHostBothDefined() {
        Publish push = Publish.builder()
            .url(Property.ofValue("amqp://example.org"))
            .host(Property.ofValue("ignore.it"))
            .build();

        assertThrows(IllegalArgumentException.class, () -> push.connectionFactory(runContextFactory.of()));
    }

    @Test
    void pushAsList() throws Exception {

        DeclareExchange.builder()
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .name(Property.ofValue("kestramqp.exchange"))
            .build()
            .run(runContextFactory.of());

        CreateQueue.builder()
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .name(Property.ofValue("kestramqp.queue"))
            .build()
            .run(runContextFactory.of());

        QueueBind.builder()
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .exchange(Property.ofValue("kestramqp.exchange"))
            .queue(Property.ofValue("kestramqp.queue"))
            .build()
            .run(runContextFactory.of());

        Publish push = Publish.builder()
            .url(Property.ofValue("amqp://guest:guest@localhost:5672/my_vhost"))
            .exchange(Property.ofValue("kestramqp.exchange"))
            .from(Arrays.asList(
                JacksonMapper.toMap(Message.builder()
                    .headers(ImmutableMap.of("testHeader", "KestraTest"))
                    .timestamp(Instant.now())
                    .data("value-1")
                    .build()),
                JacksonMapper.toMap(Message.builder()
                    .appId("unit-kestra")
                    .timestamp(Instant.now())
                    .data("{{ \"apple\" ~ \"pear\" ~ \"banana\" }}")
                    .build())
            ))
            .build();

        Publish.Output pushOutput = push.run(runContextFactory.of());
        assertThat(pushOutput.getMessagesCount(), is(2));

        Consume consume = Consume.builder()
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .queue(Property.ofValue("kestramqp.queue"))
            .maxDuration(Property.ofValue(Duration.ofSeconds(3)))
            .build();

        Consume.Output pullOutput = await()
            .pollInterval(Duration.ofMillis(300))
            .atMost(Duration.ofSeconds(10))
            .ignoreExceptions() // important: Consume might throw during first attempts
            .until(() -> {
                Consume.Output output = consume.run(runContextFactory.of());
                return (output != null && output.getCount() >= 2) ? output : null;
            }, notNullValue()); // wait until a non-null Output is returned

        assertThat(pullOutput.getCount(), greaterThanOrEqualTo(2));
    }

    @Test
    void failed() throws Exception {
        Publish push = Publish.builder()
            .url(Property.ofValue("amqp://guest:guest@localhost:5672/my_vhost"))
            .exchange(Property.ofValue("kestramqp.exchange"))
            .serdeType(Property.ofValue(SerdeType.STRING))
            .from(JacksonMapper.toMap(Message.builder()
                .headers(ImmutableMap.of("testHeader", "KestraTest"))
                .timestamp(Instant.now())
                .data("{invalid json}")
                .build())
            )
            .build();

        Publish.Output pushOutput = push.run(runContextFactory.of());

        assertThat(pushOutput.getMessagesCount(), is(1));

        Consume consume = Consume.builder()
            .url(Property.ofValue("amqp://guest:guest@localhost:5672/my_vhost"))
            .queue(Property.ofValue("kestramqp.queue"))
            .serdeType(Property.ofValue(SerdeType.JSON))
            .maxDuration(Property.ofValue(Duration.ofSeconds(3)))
            .build();

        assertThrows(JsonParseException.class, () -> {
            consume.run(runContextFactory.of());
        });
    }

    @Test
    void pushAsFileMaxRecord() throws Exception {
        URI uri = createTestFile(50000);

        Publish push = Publish.builder()
            .url(Property.ofValue("amqp://guest:guest@localhost:5672/my_vhost"))
            .exchange(Property.ofValue("kestramqp.exchange"))
            .from(uri.toString())
            .build();

        Publish.Output pushOutput = push.run(runContextFactory.of());

        assertThat(pushOutput.getMessagesCount(), is(50000));

        Consume consume = Consume.builder()
            .url(Property.ofValue("amqp://guest:guest@localhost:5672/my_vhost"))
            .queue(Property.ofValue("kestramqp.queue"))
            .maxRecords(Property.ofValue(1000))
            .build();

        Consume.Output pullOutput = await()
            .pollInterval(Duration.ofMillis(300))
            .atMost(Duration.ofSeconds(10))
            .ignoreExceptions()
            .until(() -> {
                Consume.Output output = consume.run(runContextFactory.of());
                return (output != null && output.getCount() >= 1000) ? output : null;
            }, notNullValue());

        assertThat(pullOutput.getCount(), greaterThanOrEqualTo(1000));
    }

    @Test
    void pushAsFile() throws Exception {
        URI uri = createTestFile(5);

        Publish push = Publish.builder()
            .url(Property.ofValue("amqp://guest:guest@localhost:5672/my_vhost"))
            .exchange(Property.ofValue("kestramqp.exchange"))
            .from(uri.toString())
            .build();

        Publish.Output pushOutput = push.run(runContextFactory.of());
        assertThat(pushOutput.getMessagesCount(), is(5));

        Consume consume = Consume.builder()
            .url(Property.ofValue("amqp://guest:guest@localhost:5672/my_vhost"))
            .queue(Property.ofValue("kestramqp.queue"))
            .maxDuration(Property.ofValue(Duration.ofSeconds(3)))
            .build();

        Consume.Output pullOutput = await()
            .pollInterval(Duration.ofMillis(300))
            .atMost(Duration.ofSeconds(10))
            .ignoreExceptions() // handle transient AMQP connection or queue access errors
            .until(() -> {
                Consume.Output output = consume.run(runContextFactory.of());
                return (output != null && output.getCount() >= 5) ? output : null;
            }, notNullValue());

        assertThat(pullOutput.getCount(), greaterThanOrEqualTo(5));
    }

    @Test
    void createAndBindTest() throws Exception {
        DeclareExchange declareExchange = DeclareExchange.builder()
            .url(Property.ofValue("amqp://guest:guest@localhost:5672/my_vhost"))
            .name(Property.ofValue("amqptests.exchange"))
            .build();
        CreateQueue createQueue = CreateQueue.builder()
            .url(Property.ofValue("amqp://guest:guest@localhost:5672/my_vhost"))
            .name(Property.ofValue("amqptests.queue"))
            .build();
        QueueBind queueBind = QueueBind.builder()
            .url(Property.ofValue("amqp://guest:guest@localhost:5672/my_vhost"))
            .exchange(Property.ofValue("amqptests.exchange"))
            .queue(Property.ofValue("amqptests.queue"))
            .build();

        DeclareExchange.Output createExchangeOutput = declareExchange.run(runContextFactory.of());
        CreateQueue.Output createQueueOutput = createQueue.run(runContextFactory.of());
        QueueBind.Output queueBindOutput = queueBind.run(runContextFactory.of());

        assertThat(createExchangeOutput.getExchange(), is("amqptests.exchange"));
        assertThat(createQueueOutput.getQueue(), is("amqptests.queue"));
        assertThat(queueBindOutput.getExchange(), is("amqptests.exchange"));
        assertThat(queueBindOutput.getQueue(), is("amqptests.queue"));
    }

    @BeforeAll
    void setUp() throws Exception {
        DeclareExchange declareExchange = DeclareExchange.builder()
            .url(Property.ofValue("amqp://guest:guest@localhost:5672/my_vhost"))
            .name(Property.ofValue("kestramqp.exchange"))
            .build();
        CreateQueue createQueue = CreateQueue.builder()
            .url(Property.ofValue("amqp://guest:guest@localhost:5672/my_vhost"))
            .name(Property.ofValue("kestramqp.queue"))
            .build();
        QueueBind queueBind = QueueBind.builder()
            .url(Property.ofValue("amqp://guest:guest@localhost:5672/my_vhost"))
            .exchange(Property.ofValue("kestramqp.exchange"))
            .queue(Property.ofValue("kestramqp.queue"))
            .build();

        declareExchange.run(runContextFactory.of());
        createQueue.run(runContextFactory.of());
        queueBind.run(runContextFactory.of());
    }

    URI createTestFile(Integer length) throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        OutputStream output = new FileOutputStream(tempFile);
        for (int i = 0; i < length; i++) {
            FileSerde.write(output,
                JacksonMapper.toMap(Message.builder()
                    .appId("unit-kestra")
                    .timestamp(Instant.now())
                    .data("value-" + i)
                    .build()));
        }
        return storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));
    }
}
