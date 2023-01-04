package io.kestra.plugin.amqp;

import com.rabbitmq.client.*;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwRunnable;

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
    @PluginProperty(dynamic = true)
    private String queue;

    @Schema(
            title = "Acknowledge message(s)",
            description = "If the message should be acknowledge when consumed"
    )
    @Builder.Default
    private boolean acknowledge = true;

    @Builder.Default
    private String consumerTag = "Kestra";

    private Integer maxRecords;
    private Duration maxDuration;

    @Override
    public Pull.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        ConnectionFactory factory = this.connectionFactory(runContext);

        Map<String, Integer> count = new HashMap<>();
        AtomicInteger total = new AtomicInteger();
        ZonedDateTime started = ZonedDateTime.now();

        File tempFile = runContext.tempFile(".ion").toFile();
        Thread thread = null;

        if (this.maxDuration == null && this.maxRecords == null) {
            throw new Exception("maxDuration or maxRecords must be set to avoid infinite loop");
        }

        try (BufferedOutputStream outputFile = new BufferedOutputStream(new FileOutputStream(tempFile));
             Connection connection = factory.newConnection()) {
            Channel channel = connection.createChannel();


            logger.info("AMQPull pulling from " + getUri() + " " + getQueue());

            thread = new Thread(throwRunnable(() -> {
                channel.basicConsume(
                        this.queue,
                        this.acknowledge,
                        this.consumerTag,
                        (consumerTag, message) -> {
                            FileSerde.write(outputFile, new String(message.getBody(), StandardCharsets.UTF_8));
                            total.getAndIncrement();
                            count.compute(this.queue, (s, integer) -> integer == null ? 1 : integer + 1);
                        },
                        (consumerTag) -> {
                        }
                );
            }));
            thread.setDaemon(true);
            thread.setName("amqp-consume");
            thread.start();

            while (!this.ended(total, started)) {
                Thread.sleep(100);
            }

            outputFile.flush();
        }
        return Output.builder()
                .uri(runContext.putTempFile(tempFile))
                .count(total.get())
                .build();
    }

    @SuppressWarnings("RedundantIfStatement")
    private boolean ended(AtomicInteger count, ZonedDateTime start) {
        if (this.maxRecords != null && count.get() >= this.maxRecords) {
            return true;
        }
        if (this.maxDuration != null && ZonedDateTime.now().toEpochSecond() > start.plus(this.maxDuration).toEpochSecond()) {
            return true;
        }

        return false;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
                title = "Count",
                description = "Number of row consumed"
        )
        private final Integer count;
        private final URI uri;

    }
}
