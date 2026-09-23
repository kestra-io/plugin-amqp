package io.kestra.plugin.amqp;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.amqp.models.Message;
import io.kestra.plugin.amqp.models.SerdeType;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
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
        // kill() must be non-blocking: it flags cancellation and returns immediately, it does not
        // await the hanging evaluate() (that's what evaluation.join() above is for)
        assertThat(killElapsed.toMillis(), lessThan(500L));
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

    /**
     * Reproduces the race window in {@code evaluate()} between its top {@code isActive.get()} check and
     * {@code currentTask.set(task)}: a {@code kill()} landing in that window used to be lost for the poll
     * cycle, letting the about-to-run task execute uncancelled. {@link ConditionContext#getRunContext()}
     * is called right after the top check and before the task is built/published, so killing the trigger
     * from there deterministically reproduces the window without relying on timing.
     */
    @Test
    void killLandingBetweenActiveCheckAndTaskPublicationShouldStillCancelTheTask() throws Exception {
        var suffix = IdUtils.create();
        var trigger = Trigger.builder()
            .id("watch-kill-race-" + suffix)
            .type(Trigger.class.getName())
            .host(Property.ofValue("localhost"))
            // nothing listens here: if the race were lost and the task actually ran, connecting would
            // fail fast with a real exception instead of evaluate() returning cleanly
            .port(Property.ofValue("59999"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .queue(Property.ofValue("amqpTrigger.queue"))
            .maxRecords(Property.ofValue(1))
            .build();

        var mocked = TestsUtils.mockTrigger(runContextFactory, trigger);
        var realContext = mocked.getKey();

        var killingDuringTaskBuild = new ConditionContext(
            realContext.getFlow(),
            realContext.getExecution(),
            realContext.getRunContext(),
            realContext.getVariables(),
            realContext.getMultipleConditionStorage()
        ) {
            @Override
            public RunContext getRunContext() {
                trigger.kill();
                return super.getRunContext();
            }
        };

        Optional<Execution> result = assertDoesNotThrow(
            () -> trigger.evaluate(killingDuringTaskBuild, mocked.getValue())
        );

        assertThat(result.isEmpty(), is(true));
    }

    /**
     * Reproduces the data-loss window described in review thread PRRT_kwDOItooUc6k22ni: a kill landing
     * mid-batch exits the internal {@code Await} cleanly, so {@code task.run()} returns with a partial,
     * non-zero count. Without the {@code isActive} re-check in {@code evaluate()}, that would still
     * generate an execution for a trigger that was killed. Uses a dedicated queue (not the shared
     * {@code amqpTrigger.queue}) with exactly one pre-published message, published straight to the queue
     * via the default exchange so no binding leaks into other test classes.
     */
    @Test
    void killMidBatchShouldNotGenerateExecutionAndShouldRequeueTheMessage() throws Exception {
        var suffix = IdUtils.create();
        var queue = declareQueue("amqpTrigger.queue.killmidbatch." + suffix);
        publishToQueue(queue, "kill-mid-batch-" + suffix);

        var trigger = Trigger.builder()
            .id("watch-kill-midbatch-" + suffix)
            .type(Trigger.class.getName())
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .queue(Property.ofValue(queue))
            .consumerTag(Property.ofValue("KestraTriggerKillMidBatchTest-" + suffix))
            .serdeType(Property.ofValue(SerdeType.STRING))
            // large enough that consuming the single pre-published message doesn't complete the batch on its own
            .maxRecords(Property.ofValue(100))
            .build();

        var triggerContext = TestsUtils.mockTrigger(runContextFactory, trigger);
        var result = new AtomicReference<Optional<Execution>>();
        var failure = new AtomicReference<Exception>();
        var evaluation = new Thread(() -> {
            try {
                result.set(trigger.evaluate(triggerContext.getKey(), triggerContext.getValue()));
            } catch (Exception e) {
                failure.set(e);
            }
        });
        evaluation.setDaemon(true);
        evaluation.start();

        // give RabbitMQ time to deliver the single pre-published message and the callback to consume it
        Thread.sleep(500);

        trigger.kill();
        evaluation.join(Duration.ofSeconds(30).toMillis());

        assertThat(evaluation.isAlive(), is(false));
        assertThat(failure.get(), is(nullValue()));
        assertThat(result.get(), is(notNullValue()));
        assertThat(result.get().isEmpty(), is(true));

        assertMessageIsRedelivered(queue);
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

    /**
     * Publishes directly to {@code queue} via the default exchange (routing key = queue name), so the
     * test doesn't need to declare or bind a dedicated exchange.
     */
    private void publishToQueue(String queue, String data) throws Exception {
        Publish.builder()
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .exchange(Property.ofValue(""))
            .routingKey(Property.ofValue(queue))
            .from(Arrays.asList(
                JacksonMapper.toMap(
                    Message.builder()
                        .headers(ImmutableMap.of("testHeader", "KestraTriggerKillTest"))
                        .timestamp(Instant.now())
                        .data(data)
                        .build()
                )
            ))
            .build()
            .run(runContextFactory.of());
    }

    /**
     * Consumes {@code queue} with a short-lived consumer: the message killed mid-batch must still be
     * there (or already redelivered) once the aborted connection releases it back to the broker.
     */
    private void assertMessageIsRedelivered(String queue) throws Exception {
        var consume = Consume.builder()
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .queue(Property.ofValue(queue))
            .consumerTag(Property.ofValue("KestraTriggerKillMidBatchTest-redelivery-" + IdUtils.create()))
            .serdeType(Property.ofValue(SerdeType.STRING))
            .maxRecords(Property.ofValue(1))
            .maxDuration(Property.ofValue(Duration.ofSeconds(10)))
            .build();

        var output = consume.run(runContextFactory.of());

        assertThat(output.getCount(), equalTo(1));
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
