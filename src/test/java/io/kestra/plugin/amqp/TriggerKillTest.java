package io.kestra.plugin.amqp;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.conditions.ConditionContext;
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
        var queue = declareQueue("amqpTrigger.queue.kill." + suffix);
        var trigger = triggerOnEmptyQueue("watch-kill-" + suffix, "KestraTriggerKillTest-" + suffix, queue);
        var evaluation = evaluateInBackground(trigger, TestsUtils.mockTrigger(runContextFactory, trigger));

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
        var queue = declareQueue("amqpTrigger.queue.stop." + suffix);
        var trigger = triggerOnEmptyQueue("watch-stop-" + suffix, "KestraTriggerStopTest-" + suffix, queue);
        var evaluation = evaluateInBackground(trigger, TestsUtils.mockTrigger(runContextFactory, trigger));

        Thread.sleep(500);

        var stopStartedAt = Instant.now();
        trigger.stop();
        var stopElapsed = Duration.between(stopStartedAt, Instant.now());

        assertThat(stopElapsed.toMillis(), lessThan(500L));

        evaluation.join(Duration.ofSeconds(30).toMillis());
        assertThat(evaluation.isAlive(), is(false));
    }

    private String declareQueue(String queue) throws Exception {
        CreateQueue.builder()
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .name(Property.ofValue(queue))
            .build()
            .run(runContextFactory.of());

        return queue;
    }

    private Trigger triggerOnEmptyQueue(String id, String consumerTag, String queue) {
        return Trigger.builder()
            .id(id)
            .type(Trigger.class.getName())
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .queue(Property.ofValue(queue))
            .consumerTag(Property.ofValue(consumerTag))
            .serdeType(Property.ofValue(SerdeType.STRING))
            .maxRecords(Property.ofValue(1000))
            .build();
    }

    /**
     * Runs {@code trigger.evaluate(...)} on a daemon thread: against an empty queue with a high
     * {@code maxRecords}, it would otherwise sit in the 1-minute {@code Await} inside {@link Consume}.
     */
    private Thread evaluateInBackground(Trigger trigger, Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> triggerContext) {
        Thread evaluation = new Thread(() -> {
            try {
                trigger.evaluate(triggerContext.getKey(), triggerContext.getValue());
            } catch (Exception ignored) {
                // evaluation is expected to be interrupted by the kill/stop
            }
        });
        evaluation.setDaemon(true);
        evaluation.start();

        return evaluation;
    }
}
