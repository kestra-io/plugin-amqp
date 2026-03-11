package io.kestra.plugin.amqp;

import org.slf4j.Logger;

import com.rabbitmq.client.impl.StrictExceptionHandler;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class AmqpExceptionHandler extends StrictExceptionHandler {
    Logger logger;

    @Override
    protected void log(String message, Throwable e) {
        logger.error(message, e);
        throw new RuntimeException();
    }
}
