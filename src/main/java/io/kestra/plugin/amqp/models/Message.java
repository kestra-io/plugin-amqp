package io.kestra.plugin.amqp.models;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.stream.Collectors;

import com.rabbitmq.client.BasicProperties;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;

@Value
@Builder

public class Message implements io.kestra.core.models.tasks.Output {
    @Schema(title = "MIME content type of the message body")
    String contentType;

    @Schema(title = "MIME content encoding of the message body")
    String contentEncoding;

    @Schema(title = "Arbitrary application-specific message headers")
    Map<String, Object> headers;

    @Schema(title = "Delivery mode: 1 for non-persistent, 2 for persistent")
    Integer deliveryMode;

    @Schema(title = "Message priority, 0 to 9")
    Integer priority;

    @Schema(title = "Application message identifier")
    String messageId;

    @Schema(title = "Correlation identifier used to match replies to requests")
    String correlationId;

    @Schema(title = "Queue name replies should be sent to")
    String replyTo;

    @Schema(title = "Time-to-live before the message expires")
    Duration expiration;

    @Schema(title = "Time the message was created")
    Instant timestamp;

    @Schema(title = "Application-specific message type")
    String type;

    @Schema(title = "Identifier of the user that published the message")
    String userId;

    @Schema(title = "Identifier of the publishing application")
    String appId;

    @Schema(title = "Deserialized message body")
    Object data;

    public static Message of(byte[] message, SerdeType serdeType, BasicProperties properties) throws Exception {
        return Message.builder()
            .data(serdeType.deserialize(message))
            .contentType(properties.getContentType())
            .contentEncoding(properties.getContentEncoding())
            .headers(
                properties.getHeaders() != null ? properties.getHeaders()
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, String::valueOf)) : null
            )
            .deliveryMode(properties.getDeliveryMode())
            .priority(properties.getPriority())
            .messageId(properties.getMessageId())
            .correlationId(properties.getCorrelationId())
            .replyTo(properties.getReplyTo())
            .expiration(properties.getExpiration() != null ? Duration.ofMillis(Long.parseLong(properties.getExpiration())) : null)
            .timestamp(properties.getTimestamp() != null ? properties.getTimestamp().toInstant() : null)
            .type(properties.getType())
            .userId(properties.getUserId())
            .appId(properties.getAppId())
            .build();
    }
}
