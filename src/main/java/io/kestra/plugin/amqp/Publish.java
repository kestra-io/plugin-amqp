package io.kestra.plugin.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.amqp.models.Message;
import io.kestra.plugin.amqp.models.SerdeType;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotNull;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Date;
import java.util.List;
import java.util.Map;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Publish a message to an AMQP exchange."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "url: amqp://guest:guest@localhost:5672/my_vhost",
                "exchange: kestramqp.exchange",
                "from:",
                "-  data: value-1",
                "   headers:",
                "       testHeader: KestraTest",
                "   timestamp: '2023-01-09T08:46:33.103130753Z'",
                "-  data: value-2",
                "   timestamp: '2023-01-09T08:46:33.115456977Z'",
                "   appId: unit-kestra"
            }
        )
    }
)
public class Publish extends AbstractAmqpConnection implements RunnableTask<Publish.Output> {
    @NotNull
    @PluginProperty(dynamic = true)
    @Schema(
        title = "The exchange to publish the message to"
    )
    private String exchange;

    @PluginProperty(dynamic = true)
    @Schema(
        title = "The routing key"
    )
    private String routingKey;

    @PluginProperty(dynamic = true)
    @NotNull
    @Schema(
        title = "The source of the data published.",
        description = "It can be an Kestra's internal storage URI or a list.",
        anyOf = {String.class, List.class, Object.class}
    )
    private Object from;

    @Builder.Default
    private SerdeType serdeType = SerdeType.STRING;

    @Override
    public Publish.Output run(RunContext runContext) throws Exception {
        ConnectionFactory factory = this.connectionFactory(runContext);

        try (Connection connection = factory.newConnection()) {
            Channel channel = connection.createChannel();

            Integer count = 1;
            Flowable<Message> flowable;
            Flowable<Integer> resultFlowable;

            if (this.from instanceof String) {
                if (!isValidURI((String) this.from)) {
                    throw new Exception("Invalid from parameter, must be a Kestra internal storage uri");
                }

                URI from = new URI(runContext.render((String) this.from));
                try (BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from)))) {
                    flowable = Flowable.create(FileSerde.reader(inputStream, Message.class), BackpressureStrategy.BUFFER);
                    resultFlowable = this.buildFlowable(flowable, channel, runContext);

                    count = resultFlowable
                        .reduce(Integer::sum)
                        .blockingGet();
                }

            } else if (this.from instanceof List) {
                flowable = Flowable.fromArray(((List<?>) this.from)
                    .stream()
                    .map(throwFunction(row -> {
                        if (row instanceof Map) {
                            return runContext.render((Map<String, Object>) row);
                        } else if (row instanceof String) {
                            return runContext.render((String) row);
                        } else {
                            return row;
                        }
                    })).toArray())
                    .map(o -> JacksonMapper.toMap(o, Message.class));

                resultFlowable = this.buildFlowable(flowable, channel, runContext);

                count = resultFlowable
                    .reduce(Integer::sum)
                    .blockingGet();
            } else {
                publish(channel, JacksonMapper.toMap(runContext.render((Map<String, Object>) this.from), Message.class), runContext);
            }

            channel.close();

            // metrics
            runContext.metric(Counter.of("records", count));

            return Output.builder()
                .messagesCount(count)
                .build();
        }
    }


    private Flowable<Integer> buildFlowable(Flowable<Message> flowable, Channel channel, RunContext runContext) {
        return flowable
            .map(message -> {
                publish(channel, message, runContext);
                return 1;
            });
    }


    private Boolean isValidURI(String from) {
        try {
            URI uri = new URI(from);

            return uri.getScheme().equals("kestra");
        } catch (URISyntaxException e) {
            return false;
        }
    }

    private void publish(Channel channel, Message message, RunContext runContext) throws IOException, IllegalVariableEvaluationException {
        channel.basicPublish(
            runContext.render(this.exchange),
            this.routingKey == null ? "" : runContext.render(this.routingKey),
            new AMQP.BasicProperties(
                message.getContentType(),
                message.getContentEncoding(),
                message.getHeaders(),
                message.getDeliveryMode(),
                message.getPriority(),
                message.getCorrelationId(),
                message.getReplyTo(),
                message.getExpiration() != null ? String.valueOf(message.getExpiration().toMillis()) : null,
                message.getMessageId(),
                message.getTimestamp() != null ? Date.from(message.getTimestamp()) : null,
                message.getType(),
                message.getUserId(),
                message.getAppId(),
                null
            ),
            serdeType.serialize(message.getData())
        );
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @io.swagger.v3.oas.annotations.media.Schema(
            title = "Number of messages published."
        )
        private final Integer messagesCount;
    }
}
