package io.kestra.plugin.amqp;

import com.rabbitmq.client.*;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.amqp.services.SerdeType;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Publish a message to an AMQP exchange"
)
@Plugin(
    examples = {
        @Example(
            code = {
                "uri:amqp://guest:guest@localhost:5672/my_vhost",
                "exchange:kestramqp.exchange",
                "headers:",
                "  testHeader: KestraTest",
                "from: My new message"
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

    @Schema(
        title = "The name of the queue"
    )
    private Duration expiration;

    @Schema(
        title = "The properties to add in the headers"
    )
    private Map<String, Object> headers;

    @Schema(
        title = "Determines if message will be stored on disk after broker restarts"
    )
    private Integer deliveryMode;

    @Builder.Default
    @PluginProperty(dynamic = true)
    @Schema(
        title = "The content type of the data published"
    )
    private String contentType = "application/json";

    @PluginProperty(dynamic = true)
    @NotNull
    @Schema(
        title = "The source of the data published",
        description = "Can be an internal storage uri, list or a string. If the URI is malformed, it will be considered as a string."
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
            Flowable<Object> flowable;
            Flowable<Integer> resultFlowable;

            if (this.from instanceof String || this.from instanceof List) {
                if (this.from instanceof String) {
                    if (isValidURI((String) this.from)) {
                        URI from = new URI(runContext.render((String) this.from));
                        try (BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from)))) {
                            flowable = Flowable.create(FileSerde.reader(inputStream), BackpressureStrategy.BUFFER);
                            resultFlowable = this.buildFlowable(flowable, channel, runContext);

                            count = resultFlowable
                                .reduce(Integer::sum)
                                .blockingGet();
                        }
                    } else {
                        publish(channel, serdeType.serialize(this.from), runContext);
                    }
                } else {
                    flowable = Flowable.fromArray(((List<Object>) this.from).toArray());
                    resultFlowable = this.buildFlowable(flowable, channel, runContext);

                    count = resultFlowable
                        .reduce(Integer::sum)
                        .blockingGet();
                }
                channel.close();
            }

            // metrics
            runContext.metric(Counter.of("records", count));

            return Output.builder()
                .messagesCount(count)
                .build();
        }
    }


    private Flowable<Integer> buildFlowable(Flowable<Object> flowable, Channel channel, RunContext runContext) {
        return flowable
            .map(row -> {
                publish(channel, serdeType.serialize(row), runContext);
                return 1;
            });
    }

    private Boolean isValidURI(String from) {
        try {
            new URI(from);
            return true;
        } catch (URISyntaxException e) {
            return false;
        }
    }

    private void publish(Channel channel, byte[] message, RunContext runContext) throws IOException, IllegalVariableEvaluationException {
        channel.basicPublish(
            runContext.render(this.exchange),
            this.routingKey == null ? "" : runContext.render(this.routingKey),
            new AMQP.BasicProperties(
                runContext.render(this.contentType),
                "UTF-8", this.headers,
                this.deliveryMode,
                null,
                null,
                null,
                this.expiration != null ? String.valueOf(this.expiration.toMillis()) : null,
                null,
                null,
                null,
                null,
                null,
                null
            ),
            message
        );
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @io.swagger.v3.oas.annotations.media.Schema(
            title = "Number of message published"
        )
        private final Integer messagesCount;
    }
}
