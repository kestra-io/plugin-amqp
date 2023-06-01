package io.kestra.plugin.amqp;

import com.rabbitmq.client.ConnectionFactory;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.net.URISyntaxException;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractAmqpConnection extends Task implements AmqpConnectionInterface {
    private String url;
    private String host;
    private String port;
    private String username;
    private String password;
    private String virtualHost;

    public ConnectionFactory connectionFactory(RunContext runContext) throws Exception {
        if (url != null) {
            parseFromUrl(runContext, url);
        }

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(runContext.render(host));
        factory.setPort(Integer.parseInt(runContext.render(port)));
        factory.setUsername(runContext.render(username));
        factory.setPassword(runContext.render(password));
        factory.setVirtualHost(runContext.render(virtualHost));

        factory.setExceptionHandler(new AmqpExceptionHandler(runContext.logger()));

        return factory;
    }

    void parseFromUrl(RunContext runContext, String url) throws IllegalVariableEvaluationException, URISyntaxException {
        URI amqpUri = new URI(runContext.render(url));

        host = amqpUri.getHost();
        port = String.valueOf(amqpUri.getPort());

        String auth = amqpUri.getUserInfo();
        int pos = auth.indexOf(':');
        username = pos > 0 ? auth.substring(0, pos) : auth;
        password = pos > 0 ? auth.substring(pos + 1) : "";

        if (!amqpUri.getPath().equals("")) {
            virtualHost = amqpUri.getPath();
        }
    }
}
