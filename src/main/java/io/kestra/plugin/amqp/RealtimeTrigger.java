package io.kestra.plugin.amqp;

import com.rabbitmq.client.*;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.amqp.models.Message;
import io.kestra.plugin.amqp.models.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Stream AMQP messages into real-time executions",
    description = "Creates one execution per message with manual ACK by default until the trigger is stopped, using consumer tag `Kestra` and serde `STRING` by default. Use the batch Trigger for time/volume-based batching; deprecated `url` remains for compatibility—prefer host/port/virtualHost."
)
@Plugin(
    examples = {
        @Example(
            title = "Consume a message from a AMQP queue in real-time.",
            full = true,
            code = """
                id: amqp
                namespace: company.team

                tasks:
                  - id: log
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ trigger.data }}"

                triggers:
                  - id: realtime_trigger
                    type: io.kestra.plugin.amqp.RealtimeTrigger
                    host: localhost
                    port: 5672
                    username: guest
                    password: guest
                    virtualHost: /my_vhost
                    queue: amqpTrigger.queue
                """
        )
    }
)
public class RealtimeTrigger extends AbstractTrigger implements RealtimeTriggerInterface, TriggerOutput<Message>, ConsumeBaseInterface, AmqpConnectionInterface {

    @Deprecated
    private Property<String> url;

    @NotNull
    private Property<String> host;

    @Builder.Default
    private Property<String> port = Property.ofValue("5672");

    private Property<String> username;

    private Property<String> password;

    @Builder.Default
    private Property<String> virtualHost = Property.ofValue("/");

    private Property<String> queue;

    @Builder.Default
    private Property<String> consumerTag = Property.ofValue("Kestra");

    @Builder.Default
    @Schema(
        title = "Automatic acknowledgment",
        description = """
            When true, the broker acknowledges messages as soon as they are delivered.
            When false, the trigger ACKs after emitting the execution event.
            """
    )
    private Property<Boolean> autoAck = Property.ofValue(false);

    @Builder.Default
    private Property<SerdeType> serdeType = Property.ofValue(SerdeType.STRING);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final CountDownLatch waitForTermination = new CountDownLatch(1);

    @Override
    public Publisher<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        Consume task = Consume.builder()
            .url(this.url)
            .host(this.host)
            .port(this.port)
            .username(this.username)
            .password(this.password)
            .virtualHost(this.virtualHost)
            .queue(this.queue)
            .consumerTag(this.consumerTag)
            .autoAck(this.autoAck)
            .serdeType(this.serdeType)
            .build();

        return Flux.from(publisher(task, conditionContext.getRunContext()))
            .map((record) -> TriggerService.generateRealtimeExecution(this, conditionContext, context, record));
    }

    public Publisher<Message> publisher(final Consume task, final RunContext runContext) {
        return Flux.create(
            emitter -> {
                final AtomicReference<Throwable> error = new AtomicReference<>();
                try {
                    final String queue = runContext.render(task.getQueue()).as(String.class).orElseThrow();
                    final String consumerTag = runContext.render(task.getConsumerTag()).as(String.class).orElseThrow();
                    var rAutoAck = runContext.render(task.getAutoAck()).as(Boolean.class).orElse(false);

                    ConnectionFactory factory = task.connectionFactory(runContext);
                    Connection connection = factory.newConnection();
                    Channel channel = connection.createChannel();

                    final AtomicBoolean basicCancel = new AtomicBoolean(true);
                    emitter.onDispose(() -> {
                        try {
                            if (channel.isOpen() && channel.getConnection().isOpen()) {
                                if (basicCancel.compareAndSet(true, false)) {
                                    channel.basicCancel(consumerTag); // stop consuming
                                }
                                channel.close();
                            }
                            connection.close();
                        } catch (IOException | TimeoutException e) {
                            runContext.logger().warn("Error while closing channel or connection: " + e.getMessage());
                        } finally {
                            waitForTermination.countDown();
                        }
                    });

                    DeliverCallback deliverCallback = (tag, message) -> {
                        try {
                            Message output = Message.of(message.getBody(), runContext.render(task.getSerdeType()).as(SerdeType.class).orElseThrow(), message.getProperties());
                            emitter.next(output);
                            if (!rAutoAck) {
                                channel.basicAck(message.getEnvelope().getDeliveryTag(), false);
                            }
                        } catch (Exception e) {
                            error.set(e);
                            isActive.set(false);
                        }
                    };

                    CancelCallback cancelCallback = tag -> {
                        runContext.logger().info("Consumer {} has been cancelled", consumerTag);
                        basicCancel.set(false);
                        isActive.set(false);
                    };

                    // create basic consumer
                    channel.basicConsume(
                        queue,
                        rAutoAck,
                        consumerTag,
                        deliverCallback,
                        cancelCallback,
                        (tag, sig) -> {
                        }
                    );

                    // wait for consumer to be stopped
                    busyWait();

                } catch (Throwable e) {
                    error.set(e);
                } finally {
                    // dispose
                    Throwable throwable = error.get();
                    if (throwable != null) {
                        emitter.error(throwable);
                    } else {
                        emitter.complete();
                    }
                }
            });
    }

    private void busyWait() {
        while (isActive.get()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                isActive.set(false); // proactively stop consuming
            }
        }
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void kill() {
        stop(true);
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void stop() {
        stop(false); // must be non-blocking
    }

    private void stop(boolean wait) {
        if (!isActive.compareAndSet(true, false)) {
            return;
        }

        if (wait) {
            try {
                this.waitForTermination.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
