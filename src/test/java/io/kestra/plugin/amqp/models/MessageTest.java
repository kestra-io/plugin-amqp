package io.kestra.plugin.amqp.models;

import org.junit.jupiter.api.Test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class MessageTest {

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
