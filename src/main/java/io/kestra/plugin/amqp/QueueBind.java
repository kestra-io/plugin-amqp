package io.kestra.plugin.amqp;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Bind a queue to an AMQP exchange."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: amqp_queue_bind
                namespace: company.team

                tasks:
                  - id: queue_bind
                    type: io.kestra.plugin.amqp.QueueBind
                    url: amqp://guest:guest@localhost:5672/my_vhost
                    exchange: kestramqp.exchange
                    queue: kestramqp.queue
                """
        )
    }
)
public class QueueBind extends AbstractAmqpConnection implements RunnableTask<QueueBind.Output> {
    @NotNull
    @Schema(
        title = "The exchange to bind with"
    )
    private Property<String> exchange;

    @NotNull
    @Schema(
        title = "The queue to bind"
    )
    private Property<String> queue;

    @Schema(
        title = "The routing key to use for the binding"
    )
    private Property<String> routingKey;

    @Schema(
        title = "Other properties (binding parameters)"
    )
    private Property<Map<String, Object>> args;

    @Override
    public Output run(RunContext runContext) throws Exception {
        ConnectionFactory factory = this.connectionFactory(runContext);

        String queue = runContext.render(this.queue).as(String.class).orElseThrow();
        String exchange = runContext.render(this.exchange).as(String.class).orElseThrow();

        var argsMap = runContext.render(args).asMap(String.class, Objects.class);
        try (Connection connection = factory.newConnection()) {
            Channel channel = connection.createChannel();
            channel.queueBind(queue, exchange, runContext.render(routingKey).as(String.class).orElse(""), argsMap.isEmpty() ? new HashMap<>() : argsMap);
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
