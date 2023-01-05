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
        title = "Create an Exchange",
        description = "Create an Exchange, including specified arguments"
)
public class CreateExchange extends AbstractAmqpConnection implements RunnableTask<CreateExchange.Output> {

    @NotNull
    @PluginProperty(dynamic = true)
    @Schema(
            title = "The name of the exchange"
    )
    private String name;

    @Builder.Default
    @Schema(
            title = "The exchange type"
    )
    private ExchangeType exchangeType = ExchangeType.DIRECT;

    @Builder.Default
    @Schema(
            title = "True if we are declaring a durable exchange (the exchange will survive a server restart)"
    )
    private boolean durability = true;

    @Builder.Default
    @Schema(
            title = "True if the server should delete the exchange when it is no longer in use"
    )
    private boolean autoDelete = false;

    @Builder.Default
    @Schema(
            title = "True if the exchange is internal, i.e. can't be directly published to by a client."
    )
    private boolean internal = false;

    @Schema(
            title = "Other properties (construction arguments) for the exchange"
    )
    private Map<String, Object> args;

    @Override
    public Output run(RunContext runContext) throws Exception {
        ConnectionFactory factory = this.connectionFactory(runContext);

        try(Connection connection = factory.newConnection()) {
            Channel channel = connection.createChannel();

            channel.exchangeDeclare(name, exchangeType.castToBuiltinExchangeType(), durability, autoDelete, internal, args);
        }

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