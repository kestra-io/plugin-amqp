package io.kestra.plugin.amqp;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.storages.StorageInterface;
import io.kestra.plugin.amqp.models.Message;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.*;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AMQPTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    protected StorageInterface storageInterface;

    @Test
    void pushAsList() throws Exception {
        Publish push = Publish.builder()
            .uri("amqp://guest:guest@localhost:5672/my_vhost")
            .exchange("kestramqp.exchange")
            .from(Arrays.asList(
                JacksonMapper.toMap(Message.builder()
                    .headers(ImmutableMap.of("testHeader", "KestraTest"))
                    .timestamp(Instant.now())
                    .data("value-1")
                    .build()),
                JacksonMapper.toMap(Message.builder()
                    .appId("unit-kestra")
                    .timestamp(Instant.now())
                    .data("value-2")
                    .build())
            ))
            .build();

        Publish.Output pushOutput = push.run(runContextFactory.of());

        assertThat(pushOutput.getMessagesCount(), is(2));

        Consume consume = Consume.builder()
            .uri("amqp://guest:guest@localhost:5672/my_vhost")
            .queue("kestramqp.queue")
            .maxDuration(Duration.ofSeconds(3))
            .build();

        Consume.Output pullOutput = consume.run(runContextFactory.of());
        assertThat(pullOutput.getCount(), greaterThanOrEqualTo(2));
    }

    @Test
    void pushAsFileMaxRecord() throws Exception {
        URI uri = createTestFile(50000);

        Publish push = Publish.builder()
            .uri("amqp://guest:guest@localhost:5672/my_vhost")
            .exchange("kestramqp.exchange")
            // .headers(ImmutableMap.of("testHeader", "KestraTest"))
            .from(uri.toString())
            .build();

        Publish.Output pushOutput = push.run(runContextFactory.of());

        assertThat(pushOutput.getMessagesCount(), is(50000));

        Consume consume = Consume.builder()
            .uri("amqp://guest:guest@localhost:5672/my_vhost")
            .queue("kestramqp.queue")
            .maxRecords(1000)
            .build();

        Consume.Output pullOutput = consume.run(runContextFactory.of());
        assertThat(pullOutput.getCount(), greaterThanOrEqualTo(1000));
    }

    @Test
    void pushAsFile() throws Exception {
        URI uri = createTestFile(5);

        Publish push = Publish.builder()
            .uri("amqp://guest:guest@localhost:5672/my_vhost")
            .exchange("kestramqp.exchange")
            // .headers(ImmutableMap.of("testHeader", "KestraTest"))
            .from(uri.toString())
            .build();

        Publish.Output pushOutput = push.run(runContextFactory.of());

        assertThat(pushOutput.getMessagesCount(), is(5));

        Consume consume = Consume.builder()
            .uri("amqp://guest:guest@localhost:5672/my_vhost")
            .queue("kestramqp.queue")
            .maxDuration(Duration.ofSeconds(3))
            .build();

        Consume.Output pullOutput = consume.run(runContextFactory.of());
        assertThat(pullOutput.getCount(), greaterThanOrEqualTo(5));

    }

    @Test
    void createAndBindTest() throws Exception {
        DeclareExchange declareExchange = DeclareExchange.builder()
            .uri("amqp://guest:guest@localhost:5672/my_vhost")
            .name("amqptests.exchange")
            .build();
        CreateQueue createQueue = CreateQueue.builder()
            .uri("amqp://guest:guest@localhost:5672/my_vhost")
            .name("amqptests.queue")
            .build();
        QueueBind queueBind = QueueBind.builder()
            .uri("amqp://guest:guest@localhost:5672/my_vhost")
            .exchange("amqptests.exchange")
            .queue("amqptests.queue")
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
            .uri("amqp://guest:guest@localhost:5672/my_vhost")
            .name("kestramqp.exchange")
            .build();
        CreateQueue createQueue = CreateQueue.builder()
            .uri("amqp://guest:guest@localhost:5672/my_vhost")
            .name("kestramqp.queue")
            .build();
        QueueBind queueBind = QueueBind.builder()
            .uri("amqp://guest:guest@localhost:5672/my_vhost")
            .exchange("kestramqp.exchange")
            .queue("kestramqp.queue")
            .build();

        declareExchange.run(runContextFactory.of());
        createQueue.run(runContextFactory.of());
        queueBind.run(runContextFactory.of());
    }

    URI createTestFile(Integer length) throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        File tempFile = runContext.tempFile(".ion").toFile();
        OutputStream output = new FileOutputStream(tempFile);
        for (int i = 0; i < length; i++) {
            FileSerde.write(output,
                JacksonMapper.toMap(Message.builder()
                    .appId("unit-kestra")
                    .timestamp(Instant.now())
                    .data("value-" + i)
                    .build()));
        }
        return storageInterface.put(URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));
    }
}
