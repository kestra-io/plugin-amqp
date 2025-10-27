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

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Create an AMQP queue.",
    description = "Create a queue, including specified arguments."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: amqp_create_queue
                namespace: company.team

                tasks:
                  - id: create_queue
                    type: io.kestra.plugin.amqp.CreateQueue
                    host: localhost
                    port: 5672
                    username: guest
                    password: guest
                    virtualHost: /my_vhost
                    name: kestramqp.queue
                """
        )
    }
)
public class CreateQueue extends AbstractAmqpConnection implements RunnableTask<CreateQueue.Output> {

    @NotNull
    @Schema(
        title = "The name of the queue"
    )
    private Property<String> name;

    @Builder.Default
    @Schema(
        title = "Specifies if declaring a durable queue (the queue will survive a server restart)"
    )
    private Property<Boolean> durability = Property.ofValue(true);

    @Builder.Default
    @Schema(
        title = "Specifies if declaring an exclusive queue (restricted to this connection)"
    )
    private Property<Boolean> exclusive = Property.ofValue(false);

    @Builder.Default
    @Schema(
        title = "Specifies if declaring an auto-delete queue (server will delete it when no longer in use)"
    )
    private Property<Boolean> autoDelete = Property.ofValue(false);

    @Schema(
        title = "Other properties (construction arguments) for the queue"
    )
    private Property<Map<String, Object>> args;

    @Override
    public Output run(RunContext runContext) throws Exception {
        ConnectionFactory factory = this.connectionFactory(runContext);

        try (Connection connection = factory.newConnection()) {
            Channel channel = connection.createChannel();

            var argsMap = runContext.render(args).asMap(String.class,  Object.class);

            channel.queueDeclare(runContext.render(name).as(String.class).orElseThrow(),
                runContext.render(durability).as(Boolean.class).orElseThrow(),
                runContext.render(exclusive).as(Boolean.class).orElseThrow(),
                runContext.render(autoDelete).as(Boolean.class).orElseThrow(),
                argsMap.isEmpty() ? new HashMap<>() : argsMap
            );
            channel.close();
        }

        return Output.builder().queue(runContext.render(name).as(String.class).orElseThrow()).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The queue name"
        )
        private String queue;
    }
}
