package io.kestra.plugin.amqp.models;

import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.impl.LongStringHelper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class MessageTest {

    @Test
    void shouldPreserveHeaderValuesAndNormalizeBrokerTypes() throws Exception {
        var timestamp = Instant.parse("2026-10-19T16:06:36Z");
        var headers = new LinkedHashMap<String, Object>();
        headers.put("source", LongStringHelper.asLongString("billing"));
        headers.put("retry", 3);
        headers.put("enabled", false);
        headers.put("optional", null);
        headers.put(
            "x-death", List.of(
                Map.of(
                    "reason", LongStringHelper.asLongString("expired"),
                    "count", 1L,
                    "time", Date.from(timestamp),
                    "routing-keys", List.of(LongStringHelper.asLongString("orders.wait"))
                )
            )
        );
        var properties = new AMQP.BasicProperties.Builder().headers(headers).build();

        var message = Message.of("hello".getBytes(), SerdeType.STRING, properties);

        var expected = new LinkedHashMap<String, Object>();
        expected.put("source", "billing");
        expected.put("retry", 3);
        expected.put("enabled", false);
        expected.put("optional", null);
        expected.put(
            "x-death", List.of(
                Map.of(
                    "reason", "expired",
                    "count", 1L,
                    "time", timestamp,
                    "routing-keys", List.of("orders.wait")
                )
            )
        );
        assertThat(message.getHeaders(), is(expected));
    }

    @Test
    void shouldPreserveAbsentAndEmptyHeaders() throws Exception {
        var absent = new AMQP.BasicProperties.Builder().build();
        var empty = new AMQP.BasicProperties.Builder().headers(Map.of()).build();

        assertThat(Message.of("hello".getBytes(), SerdeType.STRING, absent).getHeaders(), is(nullValue()));
        assertThat(Message.of("hello".getBytes(), SerdeType.STRING, empty).getHeaders(), is(Map.of()));
    }

    @Test
    void shouldExtractRoutingKeyFromEnvelope() throws Exception {
        Envelope envelope = new Envelope(1L, false, "my.exchange", "order.created");
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder().build();

        Message message = Message.of("hello".getBytes(), SerdeType.STRING, properties, envelope);

        assertThat(message.getRoutingKey(), is("order.created"));
        assertThat(message.getData(), is("hello"));
    }

    @Test
    void shouldHandleNullEnvelopeGracefully() throws Exception {
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder().build();

        Message message = Message.of("hello".getBytes(), SerdeType.STRING, properties, null);

        assertThat(message.getRoutingKey(), is(nullValue()));
    }
}
