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

import jakarta.validation.constraints.NotNull;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Create a queue.",
    description = "Create a queue, including specified arguments."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "url: amqp://guest:guest@localhost:5672/my_vhost",
                "name: kestramqp.queue"
            }
        )
    }
)
public class CreateQueue extends AbstractAmqpConnection implements RunnableTask<CreateQueue.Output> {

    @NotNull
    @PluginProperty(dynamic = true)
    @Schema(
        title = "The name of the queue."
    )
    private String name;

    @Builder.Default
    @Schema(
        title = "Specify if we are declaring a durable queue (the queue will survive a server restart)."
    )
    private boolean durability = true;

    @Builder.Default
    @Schema(
        title = "Specify if we are declaring an exclusive queue (restricted to this connection)."
    )
    private boolean exclusive = false;

    @Builder.Default
    @Schema(
        title = "Specify if we are declaring an auto-delete queue (server will delete it when no longer in use)."
    )
    private boolean autoDelete = false;

    @Schema(
        title = "Other properties (construction arguments) for the queue."
    )
    private Map<String, Object> args;

    @Override
    public Output run(RunContext runContext) throws Exception {
        ConnectionFactory factory = this.connectionFactory(runContext);

        try (Connection connection = factory.newConnection()) {
            Channel channel = connection.createChannel();

            channel.queueDeclare(runContext.render(name), durability, exclusive, autoDelete, args);
            channel.close();
        }

        return Output.builder().queue(runContext.render(name)).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The queue name."
        )
        private String queue;
    }
}
