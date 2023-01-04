package io.kestra.plugin.amqp.services;

import com.rabbitmq.client.BuiltinExchangeType;

public enum ExchangeType {
    DIRECT,
    FANOUT,
    HEADERS,
    TOPIC;

    public BuiltinExchangeType castToBuiltinExchangeType() {
        if (this == ExchangeType.DIRECT) {
            return BuiltinExchangeType.DIRECT;
        } else if (this == ExchangeType.FANOUT) {
            return BuiltinExchangeType.FANOUT;
        } else if (this == ExchangeType.HEADERS) {
            return BuiltinExchangeType.HEADERS;
        } else {
            return BuiltinExchangeType.TOPIC;
        }
    }
}
