package io.kestra.plugin.amqp;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownSignalException;

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

import static io.kestra.core.utils.Rethrow.throwConsumer;
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Consume AMQP messages until a stop condition",
    description = "Consumes from a queue, writing messages to internal storage and returning their URI; requires `maxDuration` or `maxRecords` to stop. Messages are ACKed in a single batch once the output is durably stored, and NACKed individually on processing failure. Defaults to consumer tag `Kestra` and serde type `STRING`."
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
                    password: "{{ secret('AMQP_PASSWORD') }}"
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
            unit = "records",
            description = "The total number of records consumed from the AMQP queue."
        )
    }
)
public class Consume extends AbstractAmqpConnection implements RunnableTask<Consume.Output>, ConsumeInterface {
    @PluginProperty(group = "main")
    private Property<String> queue;

    @Builder.Default
    @PluginProperty(group = "main")
    private Property<SerdeType> serdeType = Property.ofValue(SerdeType.STRING);

    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<String> consumerTag = Property.ofValue("Kestra");

    @Builder.Default
    @Schema(
        title = "Automatic acknowledgment",
        description = """
            When true, the broker acknowledges messages as soon as they are delivered; they are not
            requeued if the task is killed mid-batch.
            When false, the task sends a single bulk acknowledgment for the whole batch once it is
            durably stored, and NACKs a message on processing failure; if killed before that point,
            the broker requeues every unacknowledged message.
            """
    )
    @PluginProperty(group = "destination")
    private Property<Boolean> autoAck = Property.ofValue(false);

    @PluginProperty(group = "advanced")
    private Property<Integer> maxRecords;

    @PluginProperty(group = "execution")
    private Property<Duration> maxDuration;

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicReference<ConsumeThread> runningThread = new AtomicReference<>();

    /**
     * Unblocks an in-flight {@link #run(RunContext)} call, e.g. when the owning trigger is killed.
     * Must never block nor throw: the caller (a worker thread executing kill/stop) cannot afford to hang.
     */
    public void cancel() {
        if (this.cancelled.compareAndSet(false, true)) {
            var thread = this.runningThread.get();
            if (thread != null) {
                thread.abort();
            }
        }
    }

    @Override
    public Consume.Output run(RunContext runContext) throws Exception {
        if (this.cancelled.get()) {
            return Output.builder().count(0).build();
        }

        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        AtomicInteger total = new AtomicInteger();
        Instant started = Instant.now();

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
                throwConsumer(message ->
                {
                    FileSerde.write(outputFile, message);
                    total.getAndIncrement();
                }),
                () -> this.ended(total, started, rMaxRecords, rMaxDuration)
            )
        ) {
            this.runningThread.set(thread);
            try {
                thread.start();
                thread.join();
            } finally {
                this.runningThread.set(null);
            }

            if (thread.getException() != null) {
                throw thread.getException();
            }

            runContext.metric(
                Counter.of(
                    "consumed.records", total.get(),
                    "queue", runContext.render(this.queue).as(String.class).orElse(null)
                )
            );

            outputFile.flush();

            var uri = runContext.storage().putFile(tempFile);

            // Ack only after the batch is durably stored, and only if it wasn't cancelled: a kill
            // landing before this point leaves messages unacked so the broker requeues them once the
            // channel/connection is torn down, instead of losing an already-acked batch.
            if (!this.cancelled.get()) {
                thread.ackAll();
            }

            return Output.builder()
                .uri(uri)
                .count(total.get())
                .build();
        }
    }

    @SuppressWarnings("RedundantIfStatement")
    private boolean ended(AtomicInteger count, Instant start, Integer maxRecords, Duration maxDuration) {
        // Returns true if maxRecords or maxDuration reached, or the task was cancelled (e.g. trigger killed)
        if (this.cancelled.get()) {
            return true;
        }
        if (maxRecords != null && count.get() >= maxRecords) {
            return true;
        }
        if (maxDuration != null && !Instant.now().isBefore(start.plus(maxDuration))) {
            return true;
        }
        return false;
    }

    public static class ConsumeThread extends Thread implements AutoCloseable {
        // amqp-client 5.x's Connection.abort(int) still performs a synchronous, bounded close handshake
        // internally (a blocking getReply(timeoutMs)) before forcibly tearing down the socket, so it must
        // never be called with -1 (infinite) from a thread that cannot block, hence the bound below.
        private static final int CONNECTION_ABORT_TIMEOUT_MS = 5000;

        private final AtomicReference<Long> lastDeliveryTag = new AtomicReference<>();
        private final AtomicReference<Exception> exception = new AtomicReference<>();
        private final Supplier<Boolean> endSupplier;

        private final ConnectionFactory factory;
        private final RunContext runContext;
        private final ConsumeBaseInterface consumeInterface;
        @PluginProperty(group = "destination")
        private final boolean autoAck;
        private final Consumer<Message> consumer;

        // Written by this thread inside run(), read from the killing thread via abort(): must be
        // volatile so that write is visible across threads without relying on thread.join() (which
        // close(), but not abort(), waits for).
        private volatile Connection connection;
        private volatile Channel channel;

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

        /**
         * Forcibly tears down the connection (which tears down its channels), unblocking a hung
         * {@code basicConsume}/{@code Await} and releasing any unacked messages back to the broker.
         * Delegates to a short-lived daemon thread: {@link Connection#abort(int)} still runs a bounded
         * but synchronous close handshake internally, and the caller here is the worker's kill/stop
         * thread, which must never block.
         */
        void abort() {
            var conn = this.connection;
            if (conn == null) {
                return;
            }

            var aborter = new Thread(() -> conn.abort(CONNECTION_ABORT_TIMEOUT_MS), "amqp-consume-abort");
            aborter.setDaemon(true);
            aborter.start();
        }

        /**
         * Sends a single bulk ACK for every message consumed so far. Called from {@link Consume#run} after
         * the output file has been flushed and uploaded, while the channel is still open (before the
         * try-with-resources block closes it): deferring the ack until the batch is durably stored means a
         * kill or failure before this point leaves messages unacked, so the broker requeues them instead of
         * losing an already-acked batch.
         */
        void ackAll() {
            var tag = this.lastDeliveryTag.get();
            if (this.autoAck || tag == null) {
                return;
            }

            try {
                if (this.channel != null && this.channel.isOpen()) {
                    this.channel.basicAck(tag, true);
                }
            } catch (IOException | ShutdownSignalException e) {
                // the channel/connection may have been concurrently aborted (e.g. a kill landing right
                // after the cancelled check above): the messages will simply be requeued once it closes
                runContext.logger().warn("Failed to ACK the consumed batch, unacked messages will be requeued", e);
            }
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
                    (consumerTag, message) ->
                    {
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

                            consumer.accept(
                                Message.of(
                                    message.getBody(),
                                    runContext.render(consumeInterface.getSerdeType()).as(SerdeType.class).orElseThrow(),
                                    message.getProperties()
                                )
                            );

                            lastDeliveryTag.set(deliveryTag);

                            if (autoAck) {
                                runContext.logger().debug("Received message {} with auto-ack", deliveryTag);
                            } else {
                                runContext.logger().debug("Received message {}, ACK deferred until the batch is stored", deliveryTag);
                            }

                            // Check stop condition after processing
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
                    consumerTag -> runContext.logger().debug("Consumer {} cancelled", consumerTag),
                    (consumerTag, shutdownSignalException) -> runContext.logger().debug("Consumer {} shutdown signal: {}", consumerTag, shutdownSignalException.getMessage())
                );

                // Wait until stop condition or exception
                try {
                    if (!endSupplier.get()) {
                        Await.until(
                            () -> exception.get() != null || endSupplier.get(),
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
            // channel/connection can still be null here if a kill happened while factory.newConnection() was blocking
            if (channel != null) {
                try {
                    // Try to cancel, but if already cancelled, ignore the error
                    channel.basicCancel(
                        runContext.render(consumeInterface.getConsumerTag())
                            .as(String.class).orElseThrow()
                    );
                } catch (IOException e) {
                    // Ignore 'Unknown consumerTag' since it means the consumer was already cancelled
                    if (!e.getMessage().contains("Unknown consumerTag")) {
                        runContext.logger().warn("Error during consumer cancellation", e);
                    }
                } catch (ShutdownSignalException e) {
                    // channel/connection was concurrently aborted (e.g. a kill landing during this same
                    // cleanup): nothing left to cancel
                    runContext.logger().debug("Channel already closed while cancelling consumer", e);
                }
            }

            // Wait for callbacks to finish
            try {
                Await.until(
                    () -> exception.get() != null || endSupplier.get(),
                    Duration.ofMillis(200),
                    Duration.ofSeconds(2)
                );
            } catch (TimeoutException e) {
                runContext.logger().debug("Consumer shutdown wait timed out after 2 seconds.");
            }

            // Safely close channel and connection
            if (channel != null) {
                try {
                    channel.close();
                } catch (Exception e) {
                    runContext.logger().debug("Channel already closed or failed to close cleanly", e);
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                    runContext.logger().debug("Connection already closed or failed to close cleanly", e);
                }
            }

            runContext.logger().debug("Consumer closed, last delivery tag: {}", lastDeliveryTag.get());
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Total messages consumed",
            description = "Count of messages consumed before the stop condition was reached; acknowledged in a single batch once this output is durably stored."
        )
        private final Integer count;

        @Schema(
            title = "URI of file storing consumed messages",
            description = "Internal storage path to the ION-serialized batch returned as `taskrun.outputs.uri` or `trigger.uri`."
        )
        private final URI uri;
    }
}
