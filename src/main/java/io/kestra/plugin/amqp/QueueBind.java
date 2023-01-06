package io.kestra.plugin.amqp;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
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
    title = "Bind a Queue to an Exchange."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "uri: amqp://guest:guest@localhost:5672/my_vhost",
                "exchange: kestramqp.exchange",
                "queue: kestramqp.queue"
            }
        )
    }
)
public class QueueBind extends AbstractAmqpConnection implements RunnableTask<QueueBind.Output> {
    @NotNull
    @PluginProperty(dynamic = true)
    @Schema(
        title = "The exchange to bind with."
    )
    private String exchange;

    @NotNull
    @PluginProperty(dynamic = true)
    @Schema(
        title = "The queue to bind."
    )
    private String queue;

    @Schema(
        title = "The routing key to use for the binding."
    )
    private String routingKey;

    @Schema(
        title = "Other properties (binding parameters)."
    )
    private Map<String, Object> args;

    @Override
    public Output run(RunContext runContext) throws Exception {
        ConnectionFactory factory = this.connectionFactory(runContext);

        String queue = runContext.render(this.queue);
        String exchange = runContext.render(this.exchange);

        try (Connection connection = factory.newConnection()) {
            Channel channel = connection.createChannel();
            channel.queueBind(queue, exchange, routingKey == null ? "" : runContext.render(routingKey), args);
            channel.close();
        }

        return Output.builder()
            .queue(queue)
            .exchange(exchange)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The queue name"
        )
        private String queue;
        @Schema(
            title = "The exchange name"
        )
        private String exchange;
    }
}
