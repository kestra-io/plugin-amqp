package io.kestra.plugin.amqp;

import com.rabbitmq.client.ConnectionFactory;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
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
    private Property<String> url;
    private Property<String> host;
    private Property<String> port;
    private Property<String> username;
    private Property<String> password;
    private Property<String> virtualHost;

    public ConnectionFactory connectionFactory(RunContext runContext) throws Exception {
        if (url != null && host != null) {
            throw new IllegalArgumentException("Cannot define both `url` and `host`");
        }
        if (url != null) {
            parseFromUrl(runContext, runContext.render(url).as(String.class).orElseThrow());
        }

        ConnectionFactory factory = new ConnectionFactory();
        runContext.render(host).as(String.class).ifPresent(factory::setHost);
        runContext.render(port).as(String.class).map(Integer::parseInt).ifPresent(factory::setPort);
        runContext.render(username).as(String.class).ifPresent(factory::setUsername);
        runContext.render(password).as(String.class).ifPresent(factory::setPassword);
        runContext.render(virtualHost).as(String.class).ifPresent(factory::setVirtualHost);

        factory.setExceptionHandler(new AmqpExceptionHandler(runContext.logger()));

        return factory;
    }

    void parseFromUrl(RunContext runContext, String url) throws IllegalVariableEvaluationException, URISyntaxException {
        URI amqpUri = new URI(runContext.render(url));

        host = Property.of(amqpUri.getHost());
        if (amqpUri.getPort() != -1) {
            port = Property.of(String.valueOf(amqpUri.getPort()));
        }

        String auth = amqpUri.getUserInfo();
        if (auth != null) {
            int pos = auth.indexOf(':');
            username = Property.of(pos > 0 ? auth.substring(0, pos) : auth);
            password = Property.of(pos > 0 ? auth.substring(pos + 1) : "");
        }

        if (!amqpUri.getPath().isEmpty()) {
            virtualHost = Property.of(amqpUri.getPath());
        }
    }
}
