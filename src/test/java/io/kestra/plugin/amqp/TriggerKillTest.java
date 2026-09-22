package io.kestra.plugin.amqp;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.amqp.models.SerdeType;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Extends {@link AbstractTriggerTest} directly (not {@link TriggerTest}) so these tests don't inherit
 * {@code TriggerTest}'s {@code @BeforeEach} publish, which would leak unconsumed messages into the
 * shared {@code amqpTrigger.queue} and pollute {@link RealtimeTriggerTest}, which reads from it too.
 */
class TriggerKillTest extends AbstractTriggerTest {
    @Test
    void killShouldUnblockAnEvaluateHangingOnAnEmptyQueue() throws Exception {
        var suffix = IdUtils.create();
        var queue = "amqpTrigger.queue.kill." + suffix;

        CreateQueue.builder()
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .name(Property.ofValue(queue))
            .build()
            .run(runContextFactory.of());

        var trigger = Trigger.builder()
            .id("watch-kill-" + suffix)
            .type(Trigger.class.getName())
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .queue(Property.ofValue(queue))
            .consumerTag(Property.ofValue("KestraTriggerKillTest-" + suffix))
            .serdeType(Property.ofValue(SerdeType.STRING))
            .maxRecords(Property.ofValue(1000))
            .build();

        var triggerContext = TestsUtils.mockTrigger(runContextFactory, trigger);

        Thread evaluation = new Thread(() -> {
            try {
                trigger.evaluate(triggerContext.getKey(), triggerContext.getValue());
            } catch (Exception ignored) {
                // evaluation is expected to be interrupted by the kill
            }
        });
        evaluation.setDaemon(true);
        evaluation.start();

        // give the evaluation time to actually enter the blocking wait inside Consume
        Thread.sleep(500);

        var killStartedAt = Instant.now();
        trigger.kill();
        var killElapsed = Duration.between(killStartedAt, Instant.now());

        evaluation.join(Duration.ofSeconds(30).toMillis());

        assertThat(evaluation.isAlive(), is(false));
        assertThat(killElapsed.toSeconds(), lessThan(30L));
    }

    @Test
    void killBeforeAnyEvaluateShouldReturnPromptlyAndNotThrow() {
        var trigger = Trigger.builder()
            .id("watch-kill-preemptive-" + IdUtils.create())
            .type(Trigger.class.getName())
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .queue(Property.ofValue("amqpTrigger.queue"))
            .maxRecords(Property.ofValue(1))
            .build();

        var killStartedAt = Instant.now();
        assertDoesNotThrow(trigger::kill);
        var killElapsed = Duration.between(killStartedAt, Instant.now());

        assertThat(killElapsed.toMillis(), lessThan(500L));

        var triggerContext = TestsUtils.mockTrigger(runContextFactory, trigger);
        var evaluateStartedAt = Instant.now();
        Optional<Execution> result = assertDoesNotThrow(
            () -> trigger.evaluate(triggerContext.getKey(), triggerContext.getValue())
        );
        var evaluateElapsed = Duration.between(evaluateStartedAt, Instant.now());

        assertThat(result.isEmpty(), is(true));
        assertThat(evaluateElapsed.toMillis(), lessThan(500L));
    }

    @Test
    void stopShouldReturnWithoutBlocking() throws Exception {
        var suffix = IdUtils.create();
        var queue = "amqpTrigger.queue.stop." + suffix;

        CreateQueue.builder()
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .name(Property.ofValue(queue))
            .build()
            .run(runContextFactory.of());

        var trigger = Trigger.builder()
            .id("watch-stop-" + suffix)
            .type(Trigger.class.getName())
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .queue(Property.ofValue(queue))
            .consumerTag(Property.ofValue("KestraTriggerStopTest-" + suffix))
            .serdeType(Property.ofValue(SerdeType.STRING))
            .maxRecords(Property.ofValue(1000))
            .build();

        var triggerContext = TestsUtils.mockTrigger(runContextFactory, trigger);

        Thread evaluation = new Thread(() -> {
            try {
                trigger.evaluate(triggerContext.getKey(), triggerContext.getValue());
            } catch (Exception ignored) {
                // evaluation is expected to be interrupted by the stop
            }
        });
        evaluation.setDaemon(true);
        evaluation.start();

        Thread.sleep(500);

        var stopStartedAt = Instant.now();
        trigger.stop();
        var stopElapsed = Duration.between(stopStartedAt, Instant.now());

        assertThat(stopElapsed.toMillis(), lessThan(500L));

        evaluation.join(Duration.ofSeconds(30).toMillis());
        assertThat(evaluation.isAlive(), is(false));
    }
}
