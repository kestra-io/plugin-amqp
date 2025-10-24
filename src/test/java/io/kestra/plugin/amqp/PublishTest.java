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
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class PublishTest extends AbstractTest {

    @Test
    void shouldPublishMessagesToIsolatedQueue() throws Exception {
        final String suffix = IdUtils.create(); // unique id for this test run
        final String exchangeName = "amqpPublish.exchange." + suffix;
        final String queueName = "amqpPublish.queue." + suffix;

        var declareExchange = DeclareExchange.builder()
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .name(Property.ofValue(exchangeName))
            .build();
        declareExchange.run(runContextFactory.of());

        var createQueue = CreateQueue.builder()
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .name(Property.ofValue(queueName))
            .build();
        createQueue.run(runContextFactory.of());

        var queueBind = QueueBind.builder()
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .exchange(Property.ofValue(exchangeName))
            .queue(Property.ofValue(queueName))
            .build();
        queueBind.run(runContextFactory.of());

        var messages = List.of(
            Map.of(
                "data", "publish-value-1",
                "headers", Map.of("testHeader", "PublishTest"),
                "timestamp", Instant.now().toString()
            ),
            Map.of(
                "data", "publish-value-2",
                "appId", "unit-publish",
                "timestamp", Instant.now().toString()
            )
        );

        var publish = Publish.builder()
            .id("publishTest")
            .type(Publish.class.getName())
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .exchange(Property.ofValue(exchangeName))
            .serdeType(Property.ofValue(SerdeType.STRING))
            .from(messages)
            .build();

        var publishOutput = publish.run(runContextFactory.of());
        assertThat(publishOutput, is(notNullValue()));
        assertThat(publishOutput.getMessagesCount(), equalTo(2));

        var consume = Consume.builder()
            .id("consumeAfterPublish")
            .type(Consume.class.getName())
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .queue(Property.ofValue(queueName))
            .consumerTag(Property.ofValue("KestraPublishTest-" + suffix))
            .serdeType(Property.ofValue(SerdeType.STRING))
            .maxRecords(Property.ofValue(2))                       // consume only the two we just published
            .maxDuration(Property.ofValue(Duration.ofSeconds(5)))  // small guardrail
            .build();

        var consumeOutput = consume.run(runContextFactory.of());
        assertThat(consumeOutput, is(notNullValue()));
        assertThat(consumeOutput.getCount(), equalTo(2));

        URI uri = consumeOutput.getUri();

        try (var inputStream = runContextFactory.of().storage().getFile(uri);
             var reader = new BufferedReader(new InputStreamReader(inputStream), FileSerde.BUFFER_SIZE)) {

            List<Message> received = FileSerde.readAll(reader, Message.class)
                .collectList()
                .block();

            assertThat(received, is(notNullValue()));
            assertThat(received.size(), equalTo(2));

            var payloads = received.stream().map(Message::getData).toList();
            assertThat(payloads, hasItems("publish-value-1", "publish-value-2"));

            var headers = received.getFirst().getHeaders();
            assertThat(headers, is(notNullValue()));
            assertThat(headers.get("testHeader").toString(), containsString("PublishTest"));
        }
    }
}
