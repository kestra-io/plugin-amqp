package io.kestra.plugin.amqp;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.ReceiverOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientException;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.amqp.models.Amqp1Message;
import io.kestra.plugin.amqp.models.SerdeType;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Consume AMQP 1.0 messages until a stop condition",
    description = "Consumes from a queue or subscription over AMQP 1.0, writing messages to internal storage and returning their URI; requires `maxDuration` or `maxRecords`, and also stops when no message arrives within `pollDuration`. Messages are accepted once the batch is durably stored, and released back to the broker on failure or kill. Works with Azure Service Bus, ActiveMQ Artemis, RabbitMQ 4 and other AMQP 1.0 brokers; use `io.kestra.plugin.amqp.Consume` for AMQP 0.9.1."
)
@Plugin(
    examples = {
        @Example(
            title = "Consume up to 1000 messages from an ActiveMQ Artemis queue.",
            full = true,
            code = """
                id: amqp1_consume
                namespace: company.team

                tasks:
                  - id: consume
                    type: io.kestra.plugin.amqp.Amqp1Consume
                    host: localhost
                    port: 5672
                    username: artemis
                    password: "{{ secret('AMQP_PASSWORD') }}"
                    address: orders
                    maxRecords: 1000
                """
        ),
        @Example(
            title = "Consume JSON messages from an Azure Service Bus topic subscription for at most 30 seconds.",
            full = true,
            code = """
                id: azure_service_bus_consume
                namespace: company.team

                tasks:
                  - id: consume
                    type: io.kestra.plugin.amqp.Amqp1Consume
                    host: my-namespace.servicebus.windows.net
                    ssl: true
                    username: RootManageSharedAccessKey
                    password: "{{ secret('SERVICE_BUS_SAS_KEY') }}"
                    address: orders/Subscriptions/kestra
                    serdeType: JSON
                    maxDuration: PT30S
                """
        )
    },
    metrics = {
        @Metric(
            name = "consumed.records",
            type = Counter.TYPE,
            unit = "records",
            description = "The total number of records consumed from the AMQP 1.0 address."
        )
    }
)
public class Amqp1Consume extends AbstractAmqp1Connection implements RunnableTask<Amqp1Consume.Output>, Amqp1ConsumeInterface {
    // ProtonJ2's default prefetch: enough to keep the link busy without buffering much past a stop condition.
    private static final int MAX_OUTSTANDING_CREDIT = 10;
    // How often the receive loop re-checks the stop conditions, bounding the latency of kill().
    private static final Duration RECEIVE_INTERVAL = Duration.ofMillis(100);

    private Property<String> address;

    @Builder.Default
    private Property<SerdeType> serdeType = Property.ofValue(SerdeType.STRING);

    @Builder.Default
    private Property<Boolean> autoAck = Property.ofValue(false);

    private Property<Integer> maxRecords;

    private Property<Duration> maxDuration;

    @Builder.Default
    private Property<Duration> pollDuration = Property.ofValue(DEFAULT_POLL_DURATION);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicBoolean killed = new AtomicBoolean(false);

    /**
     * Stops an in-flight {@link #run(RunContext)} within {@link #RECEIVE_INTERVAL}; unaccepted messages are
     * released to the broker instead of being accepted. Never blocks.
     */
    @Override
    public void kill() {
        this.killed.set(true);
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        if (this.maxRecords == null && this.maxDuration == null) {
            throw new IllegalArgumentException("`maxRecords` or `maxDuration` must be set to bound the consumption");
        }

        var rAddress = runContext.render(this.address).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("`address` is required to consume messages"));
        var rSerdeType = runContext.render(this.serdeType).as(SerdeType.class).orElse(SerdeType.STRING);
        var rAutoAck = runContext.render(this.autoAck).as(Boolean.class).orElse(false);
        var rMaxRecords = runContext.render(this.maxRecords).as(Integer.class).orElse(null);
        var rMaxDuration = runContext.render(this.maxDuration).as(Duration.class).orElse(null);
        var rPollDuration = runContext.render(this.pollDuration).as(Duration.class).orElse(DEFAULT_POLL_DURATION);

        if (this.killed.get()) {
            return Output.builder().count(0).build();
        }

        var tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        var unaccepted = new ArrayList<Delivery>();
        var started = Instant.now();
        var count = 0;

        // credit is granted manually so the broker never sends (and locks) more than maxRecords messages
        var receiverOptions = new ReceiverOptions().creditWindow(0).autoAccept(rAutoAck);

        try (
            var client = Client.create();
            var connection = this.connect(runContext, client);
            var receiver = connection.openReceiver(rAddress, receiverOptions);
            var output = new BufferedOutputStream(new FileOutputStream(tempFile))
        ) {
            try {
                var granted = this.grantCredit(receiver, 0, rMaxRecords, MAX_OUTSTANDING_CREDIT);
                var lastReceived = started;

                while (!this.ended(count, started, lastReceived, rMaxRecords, rMaxDuration, rPollDuration)) {
                    var delivery = receiver.receive(RECEIVE_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
                    if (delivery == null) {
                        continue;
                    }
                    lastReceived = Instant.now();

                    try {
                        FileSerde.write(output, Amqp1Message.of(delivery.message(), rSerdeType));
                    } catch (Exception e) {
                        if (!rAutoAck) {
                            // counts as a failed delivery so poison messages end up dead-lettered by the broker
                            delivery.modified(true, false);
                        }
                        throw e;
                    }

                    if (!rAutoAck) {
                        unaccepted.add(delivery);
                    }
                    count++;
                    granted += this.grantCredit(receiver, granted, rMaxRecords, 1);
                }

                output.flush();
                var uri = runContext.storage().putFile(tempFile);

                if (!this.killed.get()) {
                    for (var delivery : unaccepted) {
                        delivery.accept();
                    }
                    unaccepted.clear();
                }

                runContext.metric(Counter.of("consumed.records", count, "address", rAddress));

                return Output.builder()
                    .uri(uri)
                    .count(count)
                    .build();
            } finally {
                this.release(runContext, receiver, unaccepted);
            }
        }
    }

    private int grantCredit(Receiver receiver, int granted, Integer maxRecords, int credit) throws ClientException {
        var toGrant = maxRecords == null ? credit : Math.min(credit, maxRecords - granted);
        if (toGrant > 0) {
            receiver.addCredit(toGrant);
        }
        return Math.max(toGrant, 0);
    }

    /**
     * Gives back to the broker the messages of an unfinished batch and those prefetched past the stop
     * condition, so they are redelivered right away rather than when the link closes.
     */
    private void release(RunContext runContext, Receiver receiver, List<Delivery> unaccepted) {
        try {
            for (var delivery : unaccepted) {
                delivery.release();
            }
            Delivery prefetched;
            while ((prefetched = receiver.tryReceive()) != null) {
                prefetched.release();
            }
        } catch (ClientException e) {
            runContext.logger().warn("Failed to release unaccepted messages, the broker will redeliver them once the link closes", e);
        }
    }

    @SuppressWarnings("RedundantIfStatement")
    private boolean ended(int count, Instant started, Instant lastReceived, Integer maxRecords, Duration maxDuration, Duration pollDuration) {
        var now = Instant.now();
        if (this.killed.get()) {
            return true;
        }
        if (maxRecords != null && count >= maxRecords) {
            return true;
        }
        if (maxDuration != null && !now.isBefore(started.plus(maxDuration))) {
            return true;
        }
        if (!now.isBefore(lastReceived.plus(pollDuration))) {
            return true;
        }
        return false;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Total messages consumed",
            description = "Count of messages consumed before a stop condition was reached."
        )
        private final Integer count;

        @Schema(
            title = "URI of file storing consumed messages",
            description = "Internal storage path to the ION-serialized batch, returned as `taskrun.outputs.uri` or `trigger.uri`."
        )
        private final URI uri;
    }
}
