package io.kestra.plugin.amqp;

import com.rabbitmq.client.impl.StrictExceptionHandler;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;

@AllArgsConstructor
public class AmqpExceptionHandler extends StrictExceptionHandler {
    Logger logger;

    @Override
    protected void log(String message, Throwable e) {
        logger.error(message, e);
        throw new RuntimeException();
    }
}
