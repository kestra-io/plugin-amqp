package io.kestra.plugin.amqp;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.amqp.models.SerdeType;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Poll AMQP queue into batch executions",
    description = "Polls the queue every 60 seconds by default, consumes until `maxRecords` or `maxDuration`, and launches one execution per batch with payloads stored at `trigger.uri`. Set at least one stop condition; deprecated `url` is kept for backward compatibility—use host/port/virtualHost instead."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: amqp_trigger
                namespace: company.team

                tasks:
                  - id: trigger
                    type: io.kestra.plugin.amqp.Trigger
                    host: localhost
                    port: 5672
                    username: guest
                    password: "{{ secret('AMQP_PASSWORD') }}"
                    virtualHost: /my_vhost
                    maxRecords: 2
                    queue: amqpTrigger.queue
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Consume.Output>, ConsumeInterface, AmqpConnectionInterface {

    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @Deprecated
    private Property<String> url;

    @NotNull
    private Property<String> host;

    @Builder.Default
    private Property<String> port = Property.ofValue("5672");

    private Property<String> username;

    @PluginProperty(secret = true, group = "connection")
    @ToString.Exclude
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
            When false, the trigger ACKs after processing and NACKs on failure.
            """
    )
    @PluginProperty(group = "advanced")
    private Property<Boolean> autoAck = Property.ofValue(false);

    private Property<Integer> maxRecords;

    private Property<Duration> maxDuration;

    @Builder.Default
    private Property<SerdeType> serdeType = Property.ofValue(SerdeType.STRING);

    private static final Duration TERMINATION_TIMEOUT = Duration.ofSeconds(5);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final CountDownLatch waitForTermination = new CountDownLatch(1);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicReference<Consume> currentTask = new AtomicReference<>();

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        if (!isActive.get()) {
            return Optional.empty();
        }

        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

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
            .maxRecords(this.maxRecords)
            .maxDuration(this.maxDuration)
            .serdeType(this.serdeType)
            .build();

        currentTask.set(task);

        Consume.Output run;
        try {
            run = task.run(runContext);
        } finally {
            currentTask.set(null);
            waitForTermination.countDown();
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Consumed '{}' messaged.", run.getCount());
        }

        if (run.getCount() == 0) {
            return Optional.empty();
        }

        Execution execution = TriggerService.generateExecution(this, conditionContext, context, run);

        return Optional.of(execution);
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

        Consume task = currentTask.get();
        if (task != null) {
            task.cancel();
        }

        // only await when an evaluation is actually in flight: a poll cycle may be killed between
        // evaluations, when nothing will ever count the latch down
        if (wait && task != null) {
            try {
                waitForTermination.await(TERMINATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
