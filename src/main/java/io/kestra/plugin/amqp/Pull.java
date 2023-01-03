package io.kestra.plugin.amqp;

import com.rabbitmq.client.*;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
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
import java.util.Map;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
        title = "Pull a message from an AMQP queue",
        description = "Pull a message from an AMQP queue, including specified headers"
)
public class Pull extends AbstractAmqpConnection implements RunnableTask<Pull.Output> {
    @NotNull
    private String queue;

    @Schema(
            title = "Acknowledge message(s)",
            description = "If the message should be acknowledge when consumed"
    )
    @Builder.Default
    private boolean acknowledge = true;

    @Builder.Default
    private String consumerTag = "Kestra";

    @Override
    public Pull.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        ConnectionFactory factory = this.connectionFactory(runContext);

        File tempFile = runContext.tempFile(".ion").toFile();
        BufferedOutputStream outputFile = new BufferedOutputStream(new FileOutputStream(tempFile));

        try (Connection connection = factory.newConnection()) {
            Channel channel = connection.createChannel();

            logger.info("AMQPull pulling from " + getUri() + " " + getQueue());

            Flowable<Object> flowable = Flowable.create(
                    emitter -> {
                        channel.basicConsume(
                            this.queue,
                            this.acknowledge,
                            this.consumerTag,
                            (consumerTag, message) -> {
                                emitter.onNext(new String(message.getBody()));
                            },
                            (consumerTag) -> {
                                emitter.onComplete();
                            }
                        );
                    },
                    BackpressureStrategy.BUFFER
                )
                .map(o -> {
                        logger.info(String.valueOf(o));
                        return o;
                    }
                )
                .doOnNext(row -> {
                    FileSerde.write(outputFile, String.valueOf(row));
                });
            Long count = flowable.count().blockingGet();
            logger.info("finished");
            outputFile.flush();

            return Output.builder()
                .uri(runContext.putTempFile(tempFile))
                .count(count.intValue())
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
                title = "Count",
                description = "Number of row consumed"
        )
        private final Integer count;
        private final Map<String, Object> headers;
        private final URI uri;

    }
}
