package io.kestra.plugin.amqp;

import com.rabbitmq.client.*;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.amqp.models.Message;
import io.kestra.plugin.amqp.models.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.*;
import java.net.URI;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
@Plugin(
    examples = {
        @Example(
            code = {
                "url: amqp://guest:guest@localhost:5672/my_vhost",
                "queue: kestramqp.queue",
                "maxRecords: 1000"
            }
        )
    }
)
public class Consume extends AbstractAmqpConnection implements RunnableTask<Consume.Output>, ConsumeInterface {
    private String queue;

    @Builder.Default
    private SerdeType serdeType = SerdeType.STRING;

    @Builder.Default
    private String consumerTag = "Kestra";

    private Integer maxRecords;

    private Duration maxDuration;


    @Override
    public Consume.Output run(RunContext runContext) throws Exception {
        ConnectionFactory factory = this.connectionFactory(runContext);

        AtomicInteger total = new AtomicInteger();
        ZonedDateTime started = ZonedDateTime.now();

        File tempFile = runContext.tempFile(".ion").toFile();

        if (this.maxDuration == null && this.maxRecords == null) {
            throw new Exception("maxDuration or maxRecords must be set to avoid infinite loop");
        }

        try (BufferedOutputStream outputFile = new BufferedOutputStream(new FileOutputStream(tempFile));
            Connection connection = factory.newConnection()) {

            Channel channel = connection.createChannel();

            AtomicReference<Long> lastDeliveryTag = new AtomicReference<>();
            AtomicReference<Exception> threadException = new AtomicReference<>();

            Thread thread = new Thread(throwRunnable(() -> {
                channel.basicConsume(
                    runContext.render(this.queue),
                    false,
                    this.consumerTag,
                    (consumerTag, message) -> {
                        Message msg = null;
                        try {
                            msg = Message.of(message.getBody(), serdeType, message.getProperties());
                        } catch (Exception e) {
                            threadException.set(e);
                        }
                        FileSerde.write(outputFile, msg);
                        total.getAndIncrement();

                        lastDeliveryTag.set(message.getEnvelope().getDeliveryTag());

                    },
                    (consumerTag) -> {
                    },
                    (consumerTag1, sig) -> {
                    }
                );
            }));
            thread.setDaemon(true);
            thread.setName("amqp-consume");
            thread.start();

            while (!this.ended(total, started)) {
                if (threadException.get() != null) {
                    channel.basicCancel(this.consumerTag);
                    channel.close();
                    thread.join();
                    throw threadException.get();
                }
                Thread.sleep(100);
            }
            channel.basicCancel(this.consumerTag);

            if (lastDeliveryTag.get() != null) {
                channel.basicAck(lastDeliveryTag.get(), true);
            }

            channel.close();
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
