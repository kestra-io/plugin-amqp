package io.kestra.plugin.amqp;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.amqp.models.Message;
import io.kestra.plugin.amqp.models.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static org.awaitility.Awaitility.await;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Consume messages from an AMQP queue.",
    description = "Requires `maxDuration` or `maxRecords`."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: amqp_consume
                namespace: company.team

                tasks:
                  - id: consume
                    type: io.kestra.plugin.amqp.Consume
                    url: amqp://guest:guest@localhost:5672/my_vhost
                    queue: kestramqp.queue
                    maxRecords: 1000
                """
        )
    },
    metrics = {
        @Metric(
            name = "records",
            type = Counter.TYPE,
            description = "The total number of records consumed from the AMQP queue."
        )
    }
)
public class Consume extends AbstractAmqpConnection implements RunnableTask<Consume.Output>, ConsumeInterface {
    private Property<String> queue;

    @Builder.Default
    private Property<SerdeType> serdeType = Property.ofValue(SerdeType.STRING);

    @Builder.Default
    private Property<String> consumerTag = Property.ofValue("Kestra");

    private Property<Integer> maxRecords;

    private Property<Duration> maxDuration;

    @Override
    public Consume.Output run(RunContext runContext) throws Exception {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        AtomicInteger total = new AtomicInteger();
        ZonedDateTime started = ZonedDateTime.now();

        if (this.maxDuration == null && this.maxRecords == null) {
            throw new Exception("maxDuration or maxRecords must be set to avoid infinite loop");
        }

        ConnectionFactory factory = this.connectionFactory(runContext);
        var max = runContext.render(this.maxRecords).as(Integer.class).orElse(null);
        var duration = runContext.render(maxDuration).as(Duration.class).orElse(null);
        try (
            BufferedOutputStream outputFile = new BufferedOutputStream(new FileOutputStream(tempFile));
            ConsumeThread thread = new ConsumeThread(
                factory,
                runContext,
                this,
                throwConsumer(message -> {
                    FileSerde.write(outputFile, message);
                    total.getAndIncrement();
                }),
                () -> this.ended(total, started, max, duration)
            );
        ) {
            thread.start();
            thread.join();

            if (thread.getException() != null) {
                throw thread.getException();
            }
            runContext.metric(Counter.of("records", total.get(), "queue", runContext.render(this.queue).as(String.class).orElse(null)));
            outputFile.flush();

            return Output.builder()
                .uri(runContext.storage().putFile(tempFile))
                .count(total.get())
                .build();
        }
    }

    @SuppressWarnings("RedundantIfStatement")
    private boolean ended(AtomicInteger count, ZonedDateTime start, Integer max, Duration duration) {
        if (max != null && count.get() >= max) {
            return true;
        }
        if (duration != null && ZonedDateTime.now().toEpochSecond() > start.plus(duration).toEpochSecond()) {
            return true;
        }
        return false;
    }

    public static class ConsumeThread extends Thread implements AutoCloseable {
        private final AtomicReference<Long> lastDeliveryTag = new AtomicReference<>();
        private final AtomicReference<Exception> exception = new AtomicReference<>();
        private final Supplier<Boolean> endSupplier;

        private final ConnectionFactory factory;
        private final RunContext runContext;
        private final ConsumeBaseInterface consumeInterface;
        private final Consumer<Message> consumer;

        private Connection connection;
        private Channel channel;

        public ConsumeThread(ConnectionFactory factory, RunContext runContext, ConsumeBaseInterface consumeInterface, Consumer<Message> consumer, Supplier<Boolean> supplier) {
            super("amqp-consume");
            this.setDaemon(true);
            this.factory = factory;
            this.runContext = runContext;
            this.consumeInterface = consumeInterface;
            this.consumer = consumer;
            this.endSupplier = supplier;
        }

        public Exception getException() {
            return this.exception.get();
        }

        @Override
        public void run() {
            try {

                connection = factory.newConnection();
                channel = connection.createChannel();

                channel.basicConsume(
                    runContext.render(consumeInterface.getQueue()).as(String.class).orElseThrow(),
                    false,
                    runContext.render(consumeInterface.getConsumerTag()).as(String.class).orElseThrow(),
                    (consumerTag, message) -> {
                        long deliveryTag = message.getEnvelope().getDeliveryTag();
                        try {
                            consumer.accept(Message.of(
                                message.getBody(),
                                runContext.render(consumeInterface.getSerdeType()).as(SerdeType.class).orElseThrow(),
                                message.getProperties()
                            ));

                            // individual ACK
                            channel.basicAck(deliveryTag, false);
                            lastDeliveryTag.set(deliveryTag);

                            runContext.logger().debug("Received message with tag {}", deliveryTag);
                            runContext.logger().debug("ACKed message {}", deliveryTag);
                        } catch (Exception e) {
                            // do a NACK to redeliver
                            try {
                                channel.basicNack(deliveryTag, false, true);
                            } catch (IOException ioException) {
                                runContext.logger().warn("Failed to NACK message", ioException);
                            }
                            exception.set(e);
                        }
                    },
                    (consumerTag) -> {

                    },
                    (consumerTag1, sig) -> {

                    }
                );

                await()
                    .pollInterval(Duration.ofMillis(100))
                    .until(() -> exception.get() != null || endSupplier.get());

            } catch (Exception e) {
                exception.set(e);
            }
        }

        @Override
        public void close() throws Exception {
            channel.basicCancel(runContext.render(consumeInterface.getConsumerTag()).as(String.class).orElseThrow());

            // await all callbacks finish
            await()
                .pollDelay(Duration.ofMillis(200))
                .atMost(Duration.ofSeconds(2))
                .until(() -> exception.get() != null || endSupplier.get());

            // close properly
            if (channel.isOpen()) {
                channel.close();
            }
            if (connection.isOpen()) {
                connection.close();
            }

            runContext.logger().debug("Closing consumer, last delivery tag: {}", lastDeliveryTag.get());
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Number of rows consumed"
        )
        private final Integer count;

        @Schema(
            title = "File URI containing consumed messages"
        )
        private final URI uri;
    }
}
