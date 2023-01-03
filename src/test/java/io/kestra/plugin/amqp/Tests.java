package io.kestra.plugin.amqp;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.storages.StorageInterface;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URI;
import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * This test will only test the main task, this allow you to send any input
 * parameters to your task and test the returning behaviour easily.
 */
@MicronautTest
class TestsAMQP {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    protected StorageInterface storageInterface;
    @Test
    void pushAsList() throws Exception {

        assertThat(Tools.getAMQPFactory("amqp://guest:guest@localhost:5672/"), notNullValue());

        Publish push = Publish.builder()
                .uri("amqp://guest:guest@localhost:5672")
                .exchange("kestramqp.exchange")
                .routingKey("")
                .headers(ImmutableMap.of("testHeader", "KestraTest"))
                .from(Arrays.asList(new String[]{"value-1", "value-2"}))
                .build();

        Publish.Output pushOutput = push.run(runContextFactory.of());

        assertThat(pushOutput.getMessagesCount(), is(2));

        Pull pull = Pull.builder()
                .uri("amqp://guest:guest@localhost")
                .acknowledge(true)
                .queue("kestramqp.queue")
                .build();

        Pull.Output pullOutput = pull.run(runContextFactory.of());

    }

    @Test
    void pushAsString() throws Exception {

        assertThat(Tools.getAMQPFactory("amqp://guest:guest@localhost:5672/"), notNullValue());

        Publish push = Publish.builder()
                .uri("amqp://guest:guest@localhost:5672")
                .exchange("kestramqp.exchange")
                .routingKey("")
                .headers(ImmutableMap.of("testHeader", "KestraTest"))
                .from("My new message")
                .build();

        Publish.Output pushOutput = push.run(runContextFactory.of());

        assertThat(pushOutput.getMessagesCount(), is(1));

        Pull pull = Pull.builder()
                .uri("amqp://guest:guest@localhost")
                .acknowledge(true)
                .queue("kestramqp.queue")
                .build();

        Pull.Output pullOutput = pull.run(runContextFactory.of());

    }

    @Test
    void pushAsFile() throws Exception {

        URI uri =  createTestFile();
        assertThat(Tools.getAMQPFactory("amqp://guest:guest@localhost:5672/"), notNullValue());

        Publish push = Publish.builder()
                .uri("amqp://guest:guest@localhost:5672")
                .exchange("kestramqp.exchange")
                .routingKey("")
                .headers(ImmutableMap.of("testHeader", "KestraTest"))
                .from(uri.toString())
                .build();

        Publish.Output pushOutput = push.run(runContextFactory.of());

        assertThat(pushOutput.getMessagesCount(), is(5));

        Pull pull = Pull.builder()
                .uri("amqp://guest:guest@localhost")
                .acknowledge(true)
                .queue("kestramqp.queue")
                .build();

        Pull.Output pullOutput = pull.run(runContextFactory.of());

    }

    URI createTestFile() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        File tempFile = runContext.tempFile(".ion").toFile();
        OutputStream output = new FileOutputStream(tempFile);
        for (int i = 0; i < 5; i++) {
            FileSerde.write(output, i);
        }
        return storageInterface.put(URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));
    }
}
