package io.kestra.plugin.amqp;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.storages.StorageInterface;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.*;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * This test will only test the main task, this allow you to send any input
 * parameters to your task and test the returning behaviour easily.
 */
@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AmqpTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    protected StorageInterface storageInterface;
    @Test
    void pushAsList() throws Exception {
        Publish push = Publish.builder()
                .uri("amqp://guest:guest@localhost:5672/my_vhost")
                .exchange("kestramqp.exchange")
                .routingKey("")
                .headers(ImmutableMap.of("testHeader", "KestraTest"))
                .from(Arrays.asList(new String[]{"value-1", "value-2"}))
                .build();

        Publish.Output pushOutput = push.run(runContextFactory.of());

        assertThat(pushOutput.getMessagesCount(), is(2));

        Pull pull = Pull.builder()
                .uri("amqp://guest:guest@localhost:5672/my_vhost")
                .acknowledge(true)
                .queue("kestramqp.queue")
                .maxDuration(Duration.ofSeconds(3))
                .build();

        Pull.Output pullOutput = pull.run(runContextFactory.of());
        assertThat(pullOutput.getCount(),greaterThanOrEqualTo(2));
    }
    @Test
    void pushAsFileMaxRecord() throws Exception {
        URI uri = createTestFile(50000);

        Publish push = Publish.builder()
                .uri("amqp://guest:guest@localhost:5672/my_vhost")
                .exchange("kestramqp.exchange")
                .routingKey("")
                .headers(ImmutableMap.of("testHeader", "KestraTest"))
                .from(uri.toString())
                .build();

        Publish.Output pushOutput = push.run(runContextFactory.of());

        assertThat(pushOutput.getMessagesCount(), is(50000));

        Pull pull = Pull.builder()
                .uri("amqp://guest:guest@localhost:5672/my_vhost")
                .acknowledge(true)
                .queue("kestramqp.queue")
                .maxRecords(1000)
                .build();

        Pull.Output pullOutput = pull.run(runContextFactory.of());
        assertThat(pullOutput.getCount(),greaterThanOrEqualTo(1000));
    }

    @Test
    void pushAsString() throws Exception {
        Publish push = Publish.builder()
                .uri("amqp://guest:guest@localhost:5672/my_vhost")
                .exchange("kestramqp.exchange")
                .routingKey("")
                .headers(ImmutableMap.of("testHeader", "KestraTest"))
                .from("My new message")
                .build();

        Publish.Output pushOutput = push.run(runContextFactory.of());

        assertThat(pushOutput.getMessagesCount(), is(1));

        Pull pull = Pull.builder()
                .uri("amqp://guest:guest@localhost:5672/my_vhost")
                .acknowledge(true)
                .queue("kestramqp.queue")
                .maxDuration(Duration.ofSeconds(3))
                .build();

        Pull.Output pullOutput = pull.run(runContextFactory.of());
        assertThat(pullOutput.getCount(),greaterThanOrEqualTo(1));

    }

    @Test
    void pushAsFile() throws Exception {
        URI uri = createTestFile(5);

        Publish push = Publish.builder()
                .uri("amqp://guest:guest@localhost:5672/my_vhost")
                .exchange("kestramqp.exchange")
                .routingKey("")
                .headers(ImmutableMap.of("testHeader", "KestraTest"))
                .from(uri.toString())
                .build();

        Publish.Output pushOutput = push.run(runContextFactory.of());

        assertThat(pushOutput.getMessagesCount(), is(5));

        Pull pull = Pull.builder()
                .uri("amqp://guest:guest@localhost:5672/my_vhost")
                .acknowledge(true)
                .queue("kestramqp.queue")
                .maxDuration(Duration.ofSeconds(3))
                .build();

        Pull.Output pullOutput = pull.run(runContextFactory.of());
        assertThat(pullOutput.getCount(),is(5));

    }

    @Test
    void createAndBindTest() throws Exception{
        CreateExchange createExchange = CreateExchange.builder()
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


        CreateExchange.Output createExchangeOutput = createExchange.run(runContextFactory.of());
        CreateQueue.Output createQueueOutput = createQueue.run(runContextFactory.of());
        QueueBind.Output queueBindOutput = queueBind.run(runContextFactory.of());

        assertThat(createExchangeOutput.getExchange(), is("amqptests.exchange"));
        assertThat(createQueueOutput.getQueue(), is("amqptests.queue"));
        assertThat(queueBindOutput.getExchange(), is("amqptests.exchange"));
        assertThat(queueBindOutput.getQueue(), is("amqptests.queue"));
    }

    @BeforeAll
    void setUp() throws Exception {

        CreateExchange createExchange = CreateExchange.builder()
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

        createExchange.run(runContextFactory.of());
        createQueue.run(runContextFactory.of());
        queueBind.run(runContextFactory.of());
    }

    URI createTestFile(Integer length) throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        File tempFile = runContext.tempFile(".ion").toFile();
        OutputStream output = new FileOutputStream(tempFile);
        for (int i = 0; i < length; i++) {
            FileSerde.write(output, i);
        }
        return storageInterface.put(URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));
    }
}
