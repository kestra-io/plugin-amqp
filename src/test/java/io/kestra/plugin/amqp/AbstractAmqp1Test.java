package io.kestra.plugin.amqp;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.amqp.models.Amqp1Message;

import jakarta.inject.Inject;

/**
 * Base class for tests running against the ActiveMQ Artemis broker of {@code docker-compose-ci.yml}.
 */
@KestraTest
abstract class AbstractAmqp1Test {
    protected static final String HOST = "localhost";
    protected static final int PORT = 5673;
    protected static final String USERNAME = "artemis";
    protected static final String PASSWORD = "artemis";

    @Inject
    protected RunContextFactory runContextFactory;

    protected static String randomAddress() {
        return "amqp1.test." + IdUtils.create();
    }

    protected Amqp1Publish.Amqp1PublishBuilder<?, ?> publishTask(String address) {
        return Amqp1Publish.builder()
            .id("publish-" + IdUtils.create())
            .type(Amqp1Publish.class.getName())
            .host(Property.ofValue(HOST))
            .port(Property.ofValue(PORT))
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .address(Property.ofValue(address));
    }

    protected Amqp1Consume.Amqp1ConsumeBuilder<?, ?> consumeTask(String address) {
        return Amqp1Consume.builder()
            .id("consume-" + IdUtils.create())
            .type(Amqp1Consume.class.getName())
            .host(Property.ofValue(HOST))
            .port(Property.ofValue(PORT))
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .address(Property.ofValue(address))
            .maxDuration(Property.ofValue(Duration.ofSeconds(10)))
            .pollDuration(Property.ofValue(Duration.ofSeconds(2)));
    }

    protected Amqp1Publish.Output publish(String address, Object from) throws Exception {
        var task = this.publishTask(address).from(from).build();
        return task.run(TestsUtils.mockRunContext(this.runContextFactory, task, Map.of()));
    }

    protected Amqp1Consume.Output consume(Amqp1Consume task) throws Exception {
        return task.run(TestsUtils.mockRunContext(this.runContextFactory, task, Map.of()));
    }

    protected List<Amqp1Message> read(URI uri) throws Exception {
        try (
            var inputStream = this.runContextFactory.of().storage().getFile(uri);
            var reader = new BufferedReader(new InputStreamReader(inputStream), FileSerde.BUFFER_SIZE)
        ) {
            return FileSerde.readAll(reader, Amqp1Message.class).collectList().block();
        }
    }
}
