package io.kestra.plugin.amqp;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;
import io.kestra.plugin.amqp.models.Amqp1Message;
import io.kestra.plugin.amqp.models.SerdeType;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class Amqp1ConsumeTest extends AbstractAmqp1Test {
    @Test
    void shouldConsumeAndAcceptPublishedMessages() throws Exception {
        var address = randomAddress();
        this.publish(address, List.of(Map.of("data", "value-1"), Map.of("data", "value-2")));

        var output = this.consume(this.consumeTask(address).maxRecords(Property.ofValue(2)).build());

        assertThat(output.getCount(), is(2));
        assertThat(this.read(output.getUri()).stream().map(Amqp1Message::getData).toList(), contains("value-1", "value-2"));
        assertThat(this.consume(this.consumeTask(address).build()).getCount(), is(0));
    }

    @Test
    void shouldStopAfterMaxRecordsAndLeaveRemainingMessagesOnTheBroker() throws Exception {
        var address = randomAddress();
        this.publish(address, List.of(Map.of("data", "value-1"), Map.of("data", "value-2"), Map.of("data", "value-3")));

        var first = this.consume(this.consumeTask(address).maxRecords(Property.ofValue(1)).build());
        assertThat(first.getCount(), is(1));
        assertThat(this.read(first.getUri()).getFirst().getData(), is("value-1"));

        var rest = this.consume(this.consumeTask(address).maxRecords(Property.ofValue(10)).build());
        assertThat(rest.getCount(), is(2));
        assertThat(this.read(rest.getUri()).stream().map(Amqp1Message::getData).toList(), contains("value-2", "value-3"));
    }

    @Test
    void shouldConsumeWithAutoAck() throws Exception {
        var address = randomAddress();
        this.publish(address, List.of(Map.of("data", "value-1"), Map.of("data", "value-2")));

        var output = this.consume(
            this.consumeTask(address)
                .autoAck(Property.ofValue(true))
                .maxRecords(Property.ofValue(2))
                .build()
        );

        assertThat(output.getCount(), is(2));
        assertThat(this.consume(this.consumeTask(address).build()).getCount(), is(0));
    }

    @Test
    void shouldStopWhenNoMessageArrivesWithinPollDuration() throws Exception {
        var startedAt = Instant.now();
        var output = this.consume(
            this.consumeTask(randomAddress())
                .maxDuration(Property.ofValue(Duration.ofMinutes(1)))
                .pollDuration(Property.ofValue(Duration.ofMillis(500)))
                .build()
        );

        assertThat(output.getCount(), is(0));
        assertThat(output.getUri(), is(notNullValue()));
        assertThat(Duration.between(startedAt, Instant.now()), lessThan(Duration.ofSeconds(10)));
    }

    @Test
    void shouldRespectSubSecondMaxDuration() throws Exception {
        var startedAt = Instant.now();
        var output = this.consume(
            this.consumeTask(randomAddress())
                .maxDuration(Property.ofValue(Duration.ofMillis(1)))
                .pollDuration(Property.ofValue(Duration.ofMinutes(1)))
                .build()
        );

        assertThat(output.getCount(), is(0));
        assertThat(Duration.between(startedAt, Instant.now()), lessThan(Duration.ofSeconds(5)));
    }

    @Test
    void shouldRequireAStopCondition() {
        var task = this.consumeTask(randomAddress()).maxDuration(null).build();

        var exception = assertThrows(IllegalArgumentException.class, () -> this.consume(task));
        assertThat(exception.getMessage(), containsString("maxRecords"));
    }

    @Test
    void shouldReturnImmediatelyWhenKilledBeforeRunning() throws Exception {
        var task = this.consumeTask(randomAddress()).build();
        task.kill();

        assertThat(this.consume(task).getCount(), is(0));
    }

    @Test
    void shouldReleaseMessagesWhenKilledMidBatch() throws Exception {
        var address = randomAddress();
        this.publish(address, List.of(Map.of("data", "value-1"), Map.of("data", "value-2")));

        var task = this.consumeTask(address)
            .maxDuration(Property.ofValue(Duration.ofMinutes(1)))
            .pollDuration(Property.ofValue(Duration.ofMinutes(1)))
            .build();
        var running = CompletableFuture.supplyAsync(() ->
        {
            try {
                return this.consume(task);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        Thread.sleep(Duration.ofSeconds(2));
        task.kill();

        assertThat(running.get(10, TimeUnit.SECONDS).getCount(), is(2));
        assertThat(this.consume(this.consumeTask(address).maxRecords(Property.ofValue(2)).build()).getCount(), is(2));
    }

    @Test
    void shouldRedeliverMessagesThatFailToDeserialize() throws Exception {
        var address = randomAddress();
        this.publish(address, Map.of("data", "not-json"));

        var jsonConsume = this.consumeTask(address)
            .serdeType(Property.ofValue(SerdeType.JSON))
            .maxRecords(Property.ofValue(1))
            .build();
        assertThrows(Exception.class, () -> this.consume(jsonConsume));

        var output = this.consume(this.consumeTask(address).maxRecords(Property.ofValue(1)).build());
        assertThat(output.getCount(), is(1));
        assertThat(this.read(output.getUri()).getFirst().getData(), is("not-json"));
    }

    @Test
    void shouldDecodeAmqpValueBodiesSentByOtherClients() throws Exception {
        var address = randomAddress();
        try (
            var client = Client.create();
            var connection = client.connect(HOST, PORT, new ConnectionOptions().user(USERNAME).password(PASSWORD));
            var sender = connection.openSender(address)
        ) {
            sender.send(org.apache.qpid.protonj2.client.Message.create("{\"orderId\":42}")).awaitAccepted();
            sender.send(org.apache.qpid.protonj2.client.Message.create(Map.of("orderId", 43))).awaitAccepted();
        }

        var output = this.consume(
            this.consumeTask(address)
                .serdeType(Property.ofValue(SerdeType.JSON))
                .maxRecords(Property.ofValue(2))
                .build()
        );

        assertThat(
            this.read(output.getUri()).stream().map(Amqp1Message::getData).toList(),
            contains(Map.of("orderId", 42), Map.of("orderId", 43))
        );
    }
}
