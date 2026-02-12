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
import io.kestra.core.utils.Await;
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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Consume AMQP messages until a stop condition",
    description = "Consumes from a queue with manual ACK/NACK, writing messages to internal storage and returning their URI; requires `maxDuration` or `maxRecords` to stop. Defaults to consumer tag `Kestra` and serde type `STRING`."
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
                    host: localhost
                    port: 5672
                    username: guest
                    password: guest
                    virtualHost: /my_vhost
                    queue: kestramqp.queue
                    maxRecords: 1000
                """
        )
    },
    metrics = {
        @Metric(
            name = "consumed.records",
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

    @Builder.Default
    @Schema(
        title = "Automatic acknowledgment",
        description = """
            When true, the broker acknowledges messages as soon as they are delivered.
            When false, the task ACKs after processing and NACKs on failure.
            """
    )
    private Property<Boolean> autoAck = Property.ofValue(false);

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
        var rMaxRecords = runContext.render(this.maxRecords).as(Integer.class).orElse(null);
        var rMaxDuration = runContext.render(this.maxDuration).as(Duration.class).orElse(null);
        var rAutoAck = runContext.render(this.autoAck).as(Boolean.class).orElse(false);

        try (
            BufferedOutputStream outputFile = new BufferedOutputStream(new FileOutputStream(tempFile));
            ConsumeThread thread = new ConsumeThread(
                factory,
                runContext,
                this,
                rAutoAck,
                throwConsumer(message -> {
                    FileSerde.write(outputFile, message);
                    total.getAndIncrement();
                }),
                () -> this.ended(total, started, rMaxRecords, rMaxDuration)
            )
        ) {
            thread.start();
            thread.join();

            if (thread.getException() != null) {
                throw thread.getException();
            }

            runContext.metric(Counter.of("consumed.records", total.get(),
                "queue", runContext.render(this.queue).as(String.class).orElse(null)));

            outputFile.flush();

            return Output.builder()
                .uri(runContext.storage().putFile(tempFile))
                .count(total.get())
                .build();
        }
    }

    @SuppressWarnings("RedundantIfStatement")
    private boolean ended(AtomicInteger count, ZonedDateTime start, Integer maxRecords, Duration maxDuration) {
        // Returns true if maxRecords or maxDuration reached
        if (maxRecords != null && count.get() >= maxRecords) {
            return true;
        }
        if (maxDuration != null && ZonedDateTime.now().toEpochSecond() > start.plus(maxDuration).toEpochSecond()) {
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
        private final boolean autoAck;
        private final Consumer<Message> consumer;

        private Connection connection;
        private Channel channel;

        public ConsumeThread(ConnectionFactory factory,
                             RunContext runContext,
                             ConsumeBaseInterface consumeInterface,
                             boolean autoAck,
                             Consumer<Message> consumer,
                             Supplier<Boolean> supplier) {
            super("amqp-consume");
            this.setDaemon(true);
            this.factory = factory;
            this.runContext = runContext;
            this.consumeInterface = consumeInterface;
            this.autoAck = autoAck;
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

                // Start consuming messages
                channel.basicConsume(
                    runContext.render(consumeInterface.getQueue()).as(String.class).orElseThrow(),
                    autoAck,
                    runContext.render(consumeInterface.getConsumerTag()).as(String.class).orElseThrow(),
                    (consumerTag, message) -> {
                        long deliveryTag = message.getEnvelope().getDeliveryTag();
                        try {
                            // Skip messages if stop condition is already reached
                            if (endSupplier.get()) {
                                runContext.logger().debug("Message ignored because stop condition already reached");
                                if (!autoAck) {
                                    channel.basicNack(deliveryTag, false, true);
                                }
                                return;
                            }

                            consumer.accept(Message.of(
                                message.getBody(),
                                runContext.render(consumeInterface.getSerdeType()).as(SerdeType.class).orElseThrow(),
                                message.getProperties()
                            ));

                            if (!autoAck) {
                                channel.basicAck(deliveryTag, false);
                            }
                            lastDeliveryTag.set(deliveryTag);

                            if (autoAck) {
                                runContext.logger().debug("Received message {} with auto-ack", deliveryTag);
                            } else {
                                runContext.logger().debug("Received and ACKed message {}", deliveryTag);
                            }

                            // Check stop condition after ACK
                            if (endSupplier.get()) {
                                runContext.logger().debug("Stop condition reached, cancelling consumer {}", consumerTag);
                                try {
                                    channel.basicCancel(consumerTag);
                                } catch (IOException cancelError) {
                                    // Ignore if already cancelled
                                    if (!cancelError.getMessage().contains("Unknown consumerTag")) {
                                        runContext.logger().warn("Error while cancelling consumer", cancelError);
                                    }
                                }
                            }
                        } catch (Exception e) {
                            try {
                                if (!autoAck) {
                                    channel.basicNack(deliveryTag, false, true);
                                }
                            } catch (IOException ioException) {
                                runContext.logger().warn("Failed to NACK message", ioException);
                            }
                            exception.set(e);
                        }
                    },
                    consumerTag ->
                        runContext.logger().debug("Consumer {} cancelled", consumerTag),
                    (consumerTag, shutdownSignalException) ->
                        runContext.logger().debug("Consumer {} shutdown signal: {}", consumerTag, shutdownSignalException.getMessage())
                );

                // Wait until stop condition or exception
                try {
                    if (!endSupplier.get()) {
                        Await.until(() -> exception.get() != null || endSupplier.get(),
                            Duration.ofMillis(100),
                            Duration.ofMinutes(1)
                        );
                    }
                } catch (TimeoutException e) {
                    runContext.logger().debug("No messages to process or end condition not reached within timeout, closing the connection");
                } catch (Exception e) {
                    exception.set(e);
                }

            } catch (Exception e) {
                exception.set(e);
            }
        }

        @Override
        public void close() throws Exception {
            try {
                // Try to cancel, but if already cancelled, ignore the error
                channel.basicCancel(runContext.render(consumeInterface.getConsumerTag())
                    .as(String.class).orElseThrow());
            } catch (IOException e) {
                // Ignore 'Unknown consumerTag' since it means the consumer was already cancelled
                if (!e.getMessage().contains("Unknown consumerTag")) {
                    runContext.logger().warn("Error during consumer cancellation", e);
                }
            }

            // Wait for callbacks to finish
            try {
                Await.until(() -> exception.get() != null || endSupplier.get(),
                    Duration.ofMillis(200),
                    Duration.ofSeconds(2)
                );
            } catch (TimeoutException e) {
                runContext.logger().debug("Consumer shutdown wait timed out after 2 seconds.");
            }

            // Safely close channel and connection
            try {
                channel.close();
            } catch (Exception e) {
                runContext.logger().debug("Channel already closed or failed to close cleanly", e);
            }

            try {
                connection.close();
            } catch (Exception e) {
                runContext.logger().debug("Connection already closed or failed to close cleanly", e);
            }

            runContext.logger().debug("Consumer closed, last delivery tag: {}", lastDeliveryTag.get());
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Total messages consumed",
            description = "Count of messages acknowledged before the stop condition was reached."
        )
        private final Integer count;

        @Schema(
            title = "URI of file storing consumed messages",
            description = "Internal storage path to the Ion-serialized batch returned as `taskrun.outputs.uri` or `trigger.uri`."
        )
        private final URI uri;
    }
}
