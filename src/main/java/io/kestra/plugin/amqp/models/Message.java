package io.kestra.plugin.amqp.models;

import com.rabbitmq.client.BasicProperties;
import lombok.Builder;
import lombok.Value;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.stream.Collectors;

@Value
@Builder

public class Message implements io.kestra.core.models.tasks.Output {
    String contentType;
    String contentEncoding;
    Map<String, Object> headers;
    Integer deliveryMode;
    Integer priority;
    String messageId;
    String correlationId;
    String replyTo;
    Duration expiration;
    Instant timestamp;
    String type;
    String userId;
    String appId;
    Object data;

    public static Message of(byte[] message, SerdeType serdeType, BasicProperties properties) throws Exception {
        return Message.builder()
            .data(serdeType.deserialize(message))
            .contentType(properties.getContentType())
            .contentEncoding(properties.getContentEncoding())
            .headers(properties.getHeaders() != null ? properties.getHeaders()
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
            .timestamp(properties.getTimestamp() != null ? properties.getTimestamp() .toInstant() : null)
            .type(properties.getType())
            .userId(properties.getUserId())
            .appId(properties.getAppId())
            .build();
    }
}
