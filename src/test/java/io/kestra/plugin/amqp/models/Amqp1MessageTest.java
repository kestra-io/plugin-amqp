package io.kestra.plugin.amqp.models;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

class Amqp1MessageTest {
    @Test
    void shouldKeepNormalizedApplicationPropertiesAndAnnotations() throws Exception {
        var wireMessage = org.apache.qpid.protonj2.client.Message.create("hello");
        wireMessage.property("retry", 3);
        wireMessage.property("enabled", false);
        wireMessage.annotation("delivery-count", UnsignedInteger.valueOf(4));
        wireMessage.annotation("nested", Map.of("source", Symbol.valueOf("billing"), "counts", List.of(UnsignedLong.valueOf(5))));

        var message = Amqp1Message.of(wireMessage, SerdeType.STRING);

        assertThat(message.getData(), is("hello"));
        assertThat(message.getApplicationProperties(), hasEntry("retry", 3));
        assertThat(message.getApplicationProperties(), hasEntry("enabled", false));
        assertThat(message.getMessageAnnotations(), hasEntry("delivery-count", 4L));
        assertThat(message.getMessageAnnotations(), hasEntry("nested", Map.of("source", "billing", "counts", List.of(BigInteger.valueOf(5)))));
    }
}
