package io.kestra.plugin.amqp;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.rabbitmq.client.impl.ValueWriter;

import static org.junit.jupiter.api.Assertions.*;

class PublishHeadersTest {
    @Test
    void shouldWriteNestedTemporalAndIntegerHeadersWithoutChangingTheOriginal() throws Exception {
        var instant = Instant.parse("2026-10-22T07:34:34Z");
        var timestamps = List.of(
            instant,
            instant.atOffset(ZoneOffset.ofHours(2)),
            instant.atZone(ZoneOffset.ofHours(-4)),
            LocalDateTime.ofInstant(instant, ZoneOffset.UTC)
        );
        var headers = Map.of(
            "x-death", timestamps.stream().map(time -> Map.of("time", time)).toList(),
            "count", BigInteger.valueOf(Long.MAX_VALUE),
            "minimum", BigInteger.valueOf(Long.MIN_VALUE),
            "enabled", true,
            "retry", 3
        );

        var converted = Publish.amqpHeaders(headers);
        var deaths = (List<?>) converted.get("x-death");
        for (var death : deaths) {
            assertEquals(Date.from(instant), ((Map<?, ?>) death).get("time"));
        }
        assertEquals(Long.MAX_VALUE, converted.get("count"));
        assertEquals(Long.MIN_VALUE, converted.get("minimum"));
        assertEquals(true, converted.get("enabled"));
        assertEquals(3, converted.get("retry"));
        assertSame(instant, ((Map<?, ?>) ((List<?>) headers.get("x-death")).getFirst()).get("time"));

        new ValueWriter(new DataOutputStream(new ByteArrayOutputStream())).writeTable(converted);
    }

    @Test
    void shouldPreserveMissingHeaders() {
        assertNull(Publish.amqpHeaders(null));
    }

    @Test
    void shouldRejectIntegersOutsideTheWireRangeWithoutTruncating() {
        assertThrows(ArithmeticException.class, () -> Publish.amqpHeaders(Map.of("count", BigInteger.ONE.shiftLeft(63))));
    }
}
