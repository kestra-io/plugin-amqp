package io.kestra.plugin.amqp.models;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.protonj2.types.UnsignedByte;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.UnsignedShort;

final class AmqpValues {
    private AmqpValues() {
    }

    static Object normalize(Object value) {
        return switch (value) {
            case null -> null;
            case String s -> s;
            case Boolean b -> b;
            case UnsignedLong n -> n.bigIntegerValue();
            case UnsignedByte n -> n.intValue();
            case UnsignedShort n -> n.intValue();
            case UnsignedInteger n -> n.longValue();
            case Number n when n.getClass().getName().startsWith("java.") -> n;
            case Date d -> d.toInstant();
            case Map<?, ?> map -> {
                var normalized = new LinkedHashMap<String, Object>();
                map.forEach((k, v) -> normalized.put(String.valueOf(k), normalize(v)));
                yield normalized;
            }
            case List<?> list -> list.stream().map(AmqpValues::normalize).toList();
            default -> String.valueOf(value);
        };
    }
}
