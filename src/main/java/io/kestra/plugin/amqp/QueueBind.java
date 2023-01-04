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
        title = "Push a message to an AMQP exchange",
        description = "Push a message to an AMQP exchange, including specified headers"
)
public class QueueBind extends AbstractAmqpConnection implements RunnableTask<QueueBind.Output> {

    @NotNull
    @PluginProperty(dynamic = true)
    private String exchange;

    @NotNull
    @PluginProperty(dynamic = true)
    private String queue;

    @NotNull
    @Builder.Default
    private String routingKey = "";


    private Map<String, Object> args;

    @Override
    public Output run(RunContext runContext) throws Exception {
        ConnectionFactory factory = this.connectionFactory(runContext);

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueBind(runContext.render(queue), runContext.render(exchange), routingKey, args);

        return Output.builder().queue(queue).exchange(exchange).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
                title = "Queue name"
        )
        private String queue;
        @Schema(
                title = "Exchange name"
        )
        private String exchange;
    }
}
