package io.kestra.plugin.amqp;

import com.rabbitmq.client.*;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.amqp.services.ExchangeType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotNull;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
        title = "Push a message to an AMQP exchange",
        description = "Push a message to an AMQP exchange, including specified headers"
)
public class CreateExchange extends AbstractAmqpConnection implements RunnableTask<CreateExchange.Output> {

    @NotNull
    @PluginProperty(dynamic = true)
    private String name;

    @Builder.Default
    private ExchangeType exchangeType = ExchangeType.DIRECT;

    @Builder.Default
    private boolean durability = true;

    @Builder.Default
    private boolean autoDelete = false;

    @Builder.Default
    private boolean internal = false;

    private Map<String, Object> args;

    @Override
    public Output run(RunContext runContext) throws Exception {
        ConnectionFactory factory = this.connectionFactory(runContext);

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(name, exchangeType.castToBuiltinExchangeType(), durability, autoDelete, internal, args);

        return Output.builder().exchange(name).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
                title = "Exchange name"
        )
        private String exchange;
    }
}
