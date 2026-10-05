package io.kestra.plugin.amqp.models;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.types.UnsignedByte;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.UnsignedShort;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;

/**
 * An AMQP 1.0 message as published from, or stored by, the AMQP 1.0 tasks, mapping the standard
 * header/properties sections plus application properties and message annotations.
 */
@Value
@Builder
public class Amqp1Message implements io.kestra.core.models.tasks.Output {
    private static final int MAX_PRIORITY = 255;

    @Schema(title = "Message body", description = "Serialized according to `serdeType` when publishing, deserialized the same way when consuming.")
    Object data;

    @Schema(
        title = "Application properties",
        description = "User-defined key/value pairs (AMQP 0.9.1 headers, Azure Service Bus custom properties); values must be simple types such as strings, numbers or booleans."
    )
    Map<String, Object> applicationProperties;

    @Schema(
        title = "Message annotations",
        description = "Broker-specific annotations, e.g. `x-opt-partition-key` sent to, or `x-opt-enqueued-time` received from, Azure Service Bus."
    )
    Map<String, Object> messageAnnotations;

    @Schema(title = "Application message identifier")
    String messageId;

    @Schema(title = "Correlation identifier used to match replies to requests")
    String correlationId;

    @Schema(title = "Identity of the user that produced the message")
    String userId;

    @Schema(title = "Application-specific subject (Azure Service Bus label)")
    String subject;

    @Schema(title = "Address replies should be sent to")
    String replyTo;

    @Schema(title = "MIME content type of the message body")
    String contentType;

    @Schema(title = "MIME content encoding of the message body")
    String contentEncoding;

    @Schema(title = "Group the message belongs to (Azure Service Bus session ID)")
    String groupId;

    @Schema(title = "Whether the broker must persist the message", description = "AMQP defaults to `false` when not set.")
    Boolean durable;

    @Schema(title = "Message priority, 0 to 255", description = "AMQP defaults to `4` when not set.")
    Integer priority;

    @Schema(title = "Time-to-live before the message expires")
    Duration timeToLive;

    @Schema(title = "Time the message was created")
    Instant creationTime;

    public static Amqp1Message of(org.apache.qpid.protonj2.client.Message<?> message, SerdeType serdeType) throws ClientException, IOException {
        var userId = message.userId();
        var timeToLive = message.timeToLive();
        var creationTime = message.creationTime();

        var applicationProperties = new LinkedHashMap<String, Object>();
        message.forEachProperty((key, value) -> applicationProperties.put(key, normalize(value)));
        var messageAnnotations = new LinkedHashMap<String, Object>();
        message.forEachAnnotation((key, value) -> messageAnnotations.put(key, normalize(value)));

        return Amqp1Message.builder()
            .data(decodeBody(message.body(), serdeType))
            .applicationProperties(applicationProperties.isEmpty() ? null : applicationProperties)
            .messageAnnotations(messageAnnotations.isEmpty() ? null : messageAnnotations)
            .messageId(message.messageId() != null ? String.valueOf(message.messageId()) : null)
            .correlationId(message.correlationId() != null ? String.valueOf(message.correlationId()) : null)
            .userId(userId != null ? new String(userId, StandardCharsets.UTF_8) : null)
            .subject(message.subject())
            .replyTo(message.replyTo())
            .contentType(message.contentType())
            .contentEncoding(message.contentEncoding())
            .groupId(message.groupId())
            .durable(message.durable())
            .priority(Byte.toUnsignedInt(message.priority()))
            .timeToLive(timeToLive > 0 ? Duration.ofMillis(timeToLive) : null)
            .creationTime(creationTime > 0 ? Instant.ofEpochMilli(creationTime) : null)
            .build();
    }

    /**
     * Builds the wire message carrying {@code body} as a single AMQP data section, so that any AMQP 1.0
     * consumer (including Azure Service Bus SDKs) reads it as raw bytes.
     */
    public org.apache.qpid.protonj2.client.Message<byte[]> toAmqpMessage(byte[] body) throws ClientException {
        var message = org.apache.qpid.protonj2.client.Message.create(body);

        if (messageId != null) {
            message.messageId(messageId);
        }
        if (correlationId != null) {
            message.correlationId(correlationId);
        }
        if (userId != null) {
            message.userId(userId.getBytes(StandardCharsets.UTF_8));
        }
        if (subject != null) {
            message.subject(subject);
        }
        if (replyTo != null) {
            message.replyTo(replyTo);
        }
        if (contentType != null) {
            message.contentType(contentType);
        }
        if (contentEncoding != null) {
            message.contentEncoding(contentEncoding);
        }
        if (groupId != null) {
            message.groupId(groupId);
        }
        if (durable != null) {
            message.durable(durable);
        }
        if (priority != null) {
            if (priority < 0 || priority > MAX_PRIORITY) {
                throw new IllegalArgumentException("Message priority must be between 0 and " + MAX_PRIORITY + ", got " + priority);
            }
            message.priority((byte) priority.intValue());
        }
        if (timeToLive != null) {
            message.timeToLive(timeToLive.toMillis());
        }
        if (creationTime != null) {
            message.creationTime(creationTime.toEpochMilli());
        }
        if (applicationProperties != null) {
            for (var entry : applicationProperties.entrySet()) {
                message.property(entry.getKey(), entry.getValue());
            }
        }
        if (messageAnnotations != null) {
            for (var entry : messageAnnotations.entrySet()) {
                message.annotation(entry.getKey(), entry.getValue());
            }
        }

        return message;
    }

    private static Object decodeBody(Object body, SerdeType serdeType) throws IOException {
        if (body instanceof byte[] bytes) {
            return serdeType.deserialize(bytes);
        }
        // producers such as JMS clients send text as an AMQP value section rather than raw bytes
        if (body instanceof String text && serdeType == SerdeType.JSON) {
            return serdeType.deserialize(text.getBytes(StandardCharsets.UTF_8));
        }
        return normalize(body);
    }

    /**
     * Converts AMQP-specific types (symbols, unsigned numbers, timestamps, binaries...) into values the
     * internal storage serializer and Pebble templates understand.
     */
    private static Object normalize(Object value) {
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
            case List<?> list -> list.stream().map(Amqp1Message::normalize).toList();
            default -> String.valueOf(value);
        };
    }
}
