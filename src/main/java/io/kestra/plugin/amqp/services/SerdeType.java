package io.kestra.plugin.amqp.services;

import io.kestra.core.serializers.JacksonMapper;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

@io.swagger.v3.oas.annotations.media.Schema(
    title = "Serializer / Deserializer used for the message"
)
public enum SerdeType {
    STRING,
    JSON;

    public Object deserialize(byte[] message) throws IOException {
        if (this == SerdeType.JSON) {
            return JacksonMapper.ofJson(false).readValue(message, Object.class);
        } else {
            return new String(message, Charset.defaultCharset());
        }
    }

    public byte[] serialize(Object message) throws IOException {
        if (this == SerdeType.JSON) {
            return JacksonMapper.ofJson(false).writeValueAsBytes(message);
        } else if (this == SerdeType.STRING) {
            return message.toString().getBytes(StandardCharsets.UTF_8);
        } else {
            return (byte[]) message;
        }
    }
}