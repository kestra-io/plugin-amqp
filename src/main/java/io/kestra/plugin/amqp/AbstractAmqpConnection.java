package io.kestra.plugin.amqp;

import com.rabbitmq.client.*;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractAmqpConnection extends Task {
    @Schema(
        title = "The connection string"
    )
    @NotNull
    private String uri;

    public ConnectionFactory connectionFactory(RunContext runContext) throws URISyntaxException, IllegalVariableEvaluationException {
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

        return factory;
    }
}
