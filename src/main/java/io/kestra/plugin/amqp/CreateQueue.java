package io.kestra.plugin.amqp;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
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
    title = "Create a Queue",
    description = "Create a Queue, including specified arguments"
)
public class CreateQueue extends AbstractAmqpConnection implements RunnableTask<CreateQueue.Output> {

    @NotNull
    @PluginProperty(dynamic = true)
    @Schema(
        title = "The name of the queue"
    )
    private String name;

    @Builder.Default
    @Schema(
        title = "True if we are declaring a durable queue (the queue will survive a server restart)"
    )
    private boolean durability = true;

    @Builder.Default
    @Schema(
        title = "True if we are declaring an exclusive queue (restricted to this connection)"
    )
    private boolean exclusive = false;

    @Builder.Default
    @Schema(
        title = "True if we are declaring an autodelete queue (server will delete it when no longer in use)"
    )
    private boolean autoDelete = false;

    @Schema(
        title = "Other properties (construction arguments) for the queue"
    )
    private Map<String, Object> args;

    @Override
    public Output run(RunContext runContext) throws Exception {
        ConnectionFactory factory = this.connectionFactory(runContext);

        try (Connection connection = factory.newConnection()) {
            Channel channel = connection.createChannel();

            channel.queueDeclare(name, durability, exclusive, autoDelete, args);
        }

        return Output.builder().queue(name).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Queue name"
        )
        private String queue;
    }
}
