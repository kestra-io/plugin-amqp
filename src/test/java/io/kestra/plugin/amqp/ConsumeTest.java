package io.kestra.plugin.amqp;

import io.kestra.core.models.property.Property;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.amqp.models.Message;
import io.kestra.plugin.amqp.models.SerdeType;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.time.Duration;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class ConsumeTest extends AbstractTest {

    @Test
    void shouldConsumePublishedMessages() throws Exception {
        var publishOutput = publish();
        assertThat(publishOutput, is(notNullValue()));

        // Unique ID suffix to isolate this test run
        String idSuffix = IdUtils.create();

        var consume = Consume.builder()
            .id("consumeTest-" + idSuffix)
            .type(Consume.class.getName())
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .queue(Property.ofValue("amqpTest.queue"))
            .consumerTag(Property.ofValue("KestraConsumeTest-" + idSuffix))
            .serdeType(Property.ofValue(SerdeType.STRING))
            .maxRecords(Property.ofValue(2))
            .maxDuration(Property.ofValue(Duration.ofSeconds(3)))
            .build();

        var output = consume.run(runContextFactory.of());
        assertThat(output, is(notNullValue()));
        assertThat(output.getCount(), greaterThanOrEqualTo(2));

        URI uri = output.getUri();

        try (var inputStream = runContextFactory.of().storage().getFile(uri);
             var reader = new BufferedReader(new InputStreamReader(inputStream), FileSerde.BUFFER_SIZE)) {

            // Deserialize the Ion messages into a list
            List<Message> messages = FileSerde.readAll(reader, Message.class)
                .collectList()
                .block();

            assertThat(messages, is(notNullValue()));
            assertThat(messages.size(), equalTo(output.getCount()));

            var payloads = messages.stream().map(Message::getData).toList();
            assertThat(payloads, hasItems("value-1", "value-2"));
        }
    }

    @Test
    void shouldStopAfterMaxRecords() throws Exception {
        publish();

        // Unique ID suffix to isolate this test run
        String idSuffix = IdUtils.create();

        var consume = Consume.builder()
            .id("consumeLimit-" + idSuffix)
            .type(Consume.class.getName())
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .queue(Property.ofValue("amqpTest.queue"))
            .consumerTag(Property.ofValue("KestraConsumeTest-" + idSuffix))
            .serdeType(Property.ofValue(SerdeType.STRING))
            .maxRecords(Property.ofValue(1)) // only 1 message
            .build();

        var output = consume.run(runContextFactory.of());
        assertThat(output, is(notNullValue()));
        assertThat(output.getCount(), equalTo(1));
    }

    @Test
    void shouldSuccessWithLessMessagesThanMaxRecords() throws Exception {
        publish();

        String idSuffix = IdUtils.create();

        var consume = Consume.builder()
            .id("consume-" + idSuffix)
            .type(Consume.class.getName())
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .queue(Property.ofValue("amqpTest.queue"))
            .consumerTag(Property.ofValue("KestraConsumeTest-" + idSuffix))
            .serdeType(Property.ofValue(SerdeType.STRING))
            .maxRecords(Property.ofValue(100))
            .maxDuration(Property.ofValue(Duration.ofSeconds(10)))
            .build();

        var output = consume.run(runContextFactory.of());

        assertThat(output, is(notNullValue()));
        assertThat(output.getCount(), greaterThanOrEqualTo(1));
    }

    @Test
    void shouldSuccessWhenQueueIsEmptyWithMaxRecords() throws Exception {
        var createQueue = CreateQueue.builder()
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .name(Property.ofValue("empty-queue"))
            .build();

        createQueue.run(runContextFactory.of());

        String idSuffix = IdUtils.create();

        var consume = Consume.builder()
            .id("consumeEmpty-" + idSuffix)
            .type(Consume.class.getName())
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .queue(Property.ofValue("empty-queue"))
            .consumerTag(Property.ofValue("KestraConsumeTest-" + idSuffix))
            .serdeType(Property.ofValue(SerdeType.STRING))
            .maxDuration(Property.ofValue(Duration.ofSeconds(5)))
            .maxRecords(Property.ofValue(100))
            .build();

        var output = consume.run(runContextFactory.of());
        assertThat(output, is(notNullValue()));
        assertThat(output.getCount(), equalTo(0));
    }
}