package io.kestra.plugin.amqp;

import com.rabbitmq.client.ConnectionFactory;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.net.URI;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractAmqpConnection extends Task implements AmqpConnectionInterface {
    private String uri;

    public ConnectionFactory connectionFactory(RunContext runContext) throws Exception {
        URI amqpUri = new URI(runContext.render(uri));

        String auth = amqpUri.getUserInfo();
        int pos = auth.indexOf(':');


        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(amqpUri.getHost());
        factory.setPort(amqpUri.getPort());

        String user = pos > 0 ? auth.substring(0, pos) : auth;
        String pass = pos > 0 ? auth.substring(pos + 1) : "";

        factory.setUsername(user);
        factory.setPassword(pass);

        if (!amqpUri.getPath().equals("")) {
            factory.setVirtualHost(amqpUri.getPath());
        }

        factory.setExceptionHandler(new AmqpExceptionHandler(runContext.logger()));

        return factory;
    }
}
