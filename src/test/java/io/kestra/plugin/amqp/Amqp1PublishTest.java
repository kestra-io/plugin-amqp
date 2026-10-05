package io.kestra.plugin.amqp;

import java.io.FileOutputStream;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.amqp.models.Amqp1Message;
import io.kestra.plugin.amqp.models.SerdeType;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class Amqp1PublishTest extends AbstractAmqp1Test {
    @Test
    void shouldPublishMessagesWithTheirProperties() throws Exception {
        var address = randomAddress();
        var creationTime = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        var output = this.publish(
            address, List.of(
                JacksonMapper.toMap(
                    Amqp1Message.builder()
                        .data("value-1")
                        .applicationProperties(Map.of("source", "kestra", "attempt", 3, "replay", true))
                        .messageAnnotations(Map.of("x-opt-partition-key", "partition-1"))
                        .messageId("message-1")
                        .correlationId("correlation-1")
                        .subject("subject-1")
                        .replyTo("replies")
                        .contentType("text/plain")
                        .contentEncoding("utf-8")
                        .groupId("group-1")
                        .durable(true)
                        .priority(7)
                        .timeToLive(Duration.ofMinutes(5))
                        .creationTime(creationTime)
                        .build()
                ),
                Map.of("data", "value-2")
            )
        );

        assertThat(output.getMessagesCount(), is(2));

        var consumed = this.consume(this.consumeTask(address).maxRecords(Property.ofValue(2)).build());
        assertThat(consumed.getCount(), is(2));

        var messages = this.read(consumed.getUri());
        assertThat(messages.stream().map(Amqp1Message::getData).toList(), contains("value-1", "value-2"));

        var first = messages.getFirst();
        assertThat(
            first.getApplicationProperties(), allOf(
                hasEntry("source", (Object) "kestra"),
                hasEntry("attempt", (Object) 3),
                hasEntry("replay", (Object) true)
            )
        );
        assertThat(first.getMessageAnnotations(), hasEntry("x-opt-partition-key", (Object) "partition-1"));
        assertThat(first.getMessageId(), is("message-1"));
        assertThat(first.getCorrelationId(), is("correlation-1"));
        assertThat(first.getSubject(), is("subject-1"));
        assertThat(first.getReplyTo(), is("replies"));
        assertThat(first.getContentType(), is("text/plain"));
        assertThat(first.getContentEncoding(), is("utf-8"));
        assertThat(first.getGroupId(), is("group-1"));
        assertThat(first.getDurable(), is(true));
        assertThat(first.getPriority(), is(7));
        assertThat(first.getTimeToLive(), is(Duration.ofMinutes(5)));
        assertThat(first.getCreationTime(), is(creationTime));

        var second = messages.get(1);
        assertThat(second.getApplicationProperties(), is(nullValue()));
        assertThat(second.getDurable(), is(false));
    }

    @Test
    void shouldPublishJsonMessagesFromInternalStorage() throws Exception {
        var address = randomAddress();
        var publish = this.publishTask(address)
            .serdeType(Property.ofValue(SerdeType.JSON))
            .from("{{ inputs.messages }}")
            .build();

        var file = this.runContextFactory.of().workingDir().createTempFile(".ion").toFile();
        try (var outputStream = new FileOutputStream(file)) {
            FileSerde.write(outputStream, Map.of("data", Map.of("orderId", 1, "status", "CREATED")));
            FileSerde.write(outputStream, Map.of("data", Map.of("orderId", 2, "status", "SHIPPED")));
        }
        var runContext = TestsUtils.mockRunContext(this.runContextFactory, publish, Map.of());
        var uri = runContext.storage().putFile(file);
        runContext = TestsUtils.mockRunContext(this.runContextFactory, publish, Map.of("messages", uri.toString()));

        assertThat(publish.run(runContext).getMessagesCount(), is(2));

        var consumed = this.consume(
            this.consumeTask(address)
                .serdeType(Property.ofValue(SerdeType.JSON))
                .maxRecords(Property.ofValue(2))
                .build()
        );

        assertThat(
            this.read(consumed.getUri()).stream().map(Amqp1Message::getData).toList(),
            contains(
                Map.of("orderId", 1, "status", "CREATED"),
                Map.of("orderId", 2, "status", "SHIPPED")
            )
        );
    }

    @Test
    void shouldRejectOutOfRangePriority() {
        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> this.publish(randomAddress(), Map.of("data", "value", "priority", 256))
        );

        assertThat(exception.getMessage(), containsString("between 0 and 255"));
    }
}
