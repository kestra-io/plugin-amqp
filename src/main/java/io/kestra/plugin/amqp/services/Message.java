package io.kestra.plugin.amqp.services;

import com.rabbitmq.client.BasicProperties;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
public class Message {
    private final String contentType;
    private final Map<String, String> headers;
    private final Integer deliveryMode;
    private final Integer priority;
    private final String messageId;
    private final Instant timestamp;
    private final Object data;

    public Message(byte[] message, SerdeType serdeType, BasicProperties properties) throws IOException {
        data = serdeType.deserialize(message);
        contentType = properties.getContentType();
        headers = properties.getHeaders()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, String::valueOf));
        deliveryMode = properties.getDeliveryMode();
        priority = properties.getPriority();
        messageId = properties.getMessageId();
        timestamp = properties.getTimestamp().toInstant();
    }
}
