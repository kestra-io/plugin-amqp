package io.kestra.plugin.amqp.services;

import com.rabbitmq.client.BasicProperties;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.stream.Collectors;

@Schema(
    title = "Custom object from data consume"
)
@Getter
public class Message {
    private String contentType;
    private Map<String, String> headers;
    private Integer deliveryMode;
    private Integer priority;
    private String messageId;
    private Date timestamp;
    private Object data;

    public Message(byte[] message, SerdeType serdeType, BasicProperties properties) throws IOException {
        data = serdeType.deserialize(message);
        contentType = properties.getContentType();
        headers = properties.getHeaders().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> String.valueOf(e) ));
        deliveryMode = properties.getDeliveryMode();
        priority = properties.getPriority();
        messageId = properties.getMessageId();
        timestamp = properties.getTimestamp();
    }
}
