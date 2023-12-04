package io.kestra.plugin.amqp;

import com.rabbitmq.client.ConnectionFactory;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import java.util.Optional;
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
        Optional.ofNullable(runContext.render(host)).ifPresent(factory::setHost);
        Optional.ofNullable(runContext.render(port)).map(Integer::parseInt).ifPresent(factory::setPort);
        Optional.ofNullable(runContext.render(username)).ifPresent(factory::setUsername);
        Optional.ofNullable(runContext.render(password)).ifPresent(factory::setPassword);
        Optional.ofNullable(runContext.render(virtualHost)).ifPresent(factory::setVirtualHost);

        factory.setExceptionHandler(new AmqpExceptionHandler(runContext.logger()));

        return factory;
    }

    void parseFromUrl(RunContext runContext, String url) throws IllegalVariableEvaluationException, URISyntaxException {
        URI amqpUri = new URI(runContext.render(url));

        host = amqpUri.getHost();
        if (amqpUri.getPort() != -1) {
            port = String.valueOf(amqpUri.getPort());
        }

        String auth = amqpUri.getUserInfo();
        if (auth != null) {
            int pos = auth.indexOf(':');
            username = pos > 0 ? auth.substring(0, pos) : auth;
            password = pos > 0 ? auth.substring(pos + 1) : "";
        }

        if (!amqpUri.getPath().isEmpty()) {
            virtualHost = amqpUri.getPath();
        }
    }
}
