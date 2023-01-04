package io.kestra.plugin.amqp;

import com.rabbitmq.client.*;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Push a message to an AMQP exchange",
    description = "Push a message to an AMQP exchange, including specified headers"
)
public class Publish extends AbstractAmqpConnection implements RunnableTask<Publish.Output> {
    @NotNull
    @PluginProperty(dynamic = true)
    private String exchange;

    @Builder.Default
    @PluginProperty(dynamic = true)
    private String routingKey = "";

    @Builder.Default
    private String expiration = null;

    private Map<String, Object> headers;

    @Builder.Default
    private String data = "";

    @Builder.Default
    private String contentType = "application/json";

    @PluginProperty(dynamic = true)
    @NotNull
    private Object from;

    @Override
    public Publish.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        ConnectionFactory factory = this.connectionFactory(runContext);

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        logger.debug("AMQPush pushing to " + getUri() + " " + getExchange());

        Integer count = 1;
        Flowable<Object> flowable;
        Flowable<Integer> resultFlowable;

        if (this.from instanceof String || this.from instanceof List) {
            if(this.from instanceof String) {
                if(isValidURI((String) this.from)) {
                    URI from = new URI(runContext.render((String) this.from));
                    try (BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from)))) {
                        flowable = Flowable.create(FileSerde.reader(inputStream), BackpressureStrategy.BUFFER);
                        resultFlowable = this.buildFlowable(flowable, channel);

                        count = resultFlowable
                                .reduce(Integer::sum)
                                .blockingGet();
                    }
                }
                else {
                    String message = (String) this.from;
                    publish(channel, message);
                }
            }
            else {
                flowable = Flowable.fromArray(((List<Object>) this.from).toArray());
                resultFlowable = this.buildFlowable(flowable, channel);

                count = resultFlowable
                        .reduce(Integer::sum)
                        .blockingGet();
            }
        }

        // metrics
        runContext.metric(Counter.of("records", count));

        channel.close();
        connection.close();

        return Output.builder()
            .messagesCount(count)
            .build();
    }


    private Flowable<Integer> buildFlowable(Flowable<Object> flowable, Channel channel){
        return flowable
            .map(row -> {
                String message = String.valueOf(row);
                publish(channel, message);
                return 1;
            });
    }

    private Boolean isValidURI(String from){
        try {
            new URI(from);
            return true;
        } catch (URISyntaxException e) {
            return false;
        }
    }

    private void publish(Channel channel, String message) throws IOException {
        channel.basicPublish(
                getExchange(),
                getRoutingKey(),
                new AMQP.BasicProperties(this.contentType, "UTF-8", getHeaders(), null, null, null, null, getExpiration(), null, null, null, null, null, null),
                message.getBytes()
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
