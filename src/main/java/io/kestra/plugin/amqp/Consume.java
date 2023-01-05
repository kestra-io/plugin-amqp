package io.kestra.plugin.amqp;

import com.rabbitmq.client.*;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.amqp.services.Message;
import io.kestra.plugin.amqp.services.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.*;
import java.net.URI;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicInteger;

import static io.kestra.core.utils.Rethrow.throwRunnable;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Consume messages from an AMQP queue.",
    description = "Required a maxDuration or a maxRecords."
)
public class Consume extends AbstractAmqpConnection implements RunnableTask<Consume.Output>, ConsumeInterface {

    private String queue;

    @Builder.Default
    private String consumerTag = "Kestra";

    private Integer maxRecords;

    private Duration maxDuration;

    @Builder.Default
    private SerdeType serdeType = SerdeType.STRING;

    @Override
    public Consume.Output run(RunContext runContext) throws Exception {
        ConnectionFactory factory = this.connectionFactory(runContext);

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

            thread = new Thread(throwRunnable(() -> {
                channel.basicConsume(
                    runContext.render(this.queue),
                    false,
                    this.consumerTag,
                    (consumerTag, message) -> {
                        Message msg = new Message(message.getBody(), serdeType, message.getProperties());
                        FileSerde.write(outputFile, msg);
                        total.getAndIncrement();
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
            channel.basicCancel(this.consumerTag);
            channel.close();
            while(channel.isOpen()){
                wait(100);
            }
            thread.join();

            runContext.metric(Counter.of("records", total.get(), "queue", runContext.render(this.queue)));
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
            title = "Number of row consumed."
        )
        private final Integer count;
        @Schema(
            title = "File URI containing consumed message."
        )
        private final URI uri;

    }
}
