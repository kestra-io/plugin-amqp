package io.kestra.plugin.amqp;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.TestsUtils;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.amqp.models.Message;
import io.kestra.plugin.amqp.models.SerdeType;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.kestra.core.utils.Rethrow.throwRunnable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

class TriggerTest extends AbstractTriggerTest {
    @Test
    void flow() throws Exception {
        CountDownLatch queueCount = new CountDownLatch(1);

        Flux<Execution> receive = TestsUtils.receive(executionQueue, execution -> {
            queueCount.countDown();
            assertThat(execution.getLeft().getFlowId(), is("trigger"));
        });

        this.run("trigger.yaml", throwRunnable(() -> {
            publish();

            boolean await = queueCount.await(1, TimeUnit.MINUTES);
            assertThat(await, is(true));

            Integer trigger = (Integer) receive.blockLast().getTrigger().getVariables().get("count");

            assertThat(trigger, greaterThanOrEqualTo(2));
        }));
    }

    @Test
    void shouldConsumeMessagePublishedInBoundaryWindow() throws Exception {
        var suffix = IdUtils.create();
        var exchange = "amqpTrigger.exchange." + suffix;
        var queue = "amqpTrigger.queue." + suffix;
        var routingKey = "amqpTrigger.rk." + suffix;

        DeclareExchange.builder()
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .name(Property.ofValue(exchange))
            .build()
            .run(runContextFactory.of());

        CreateQueue.builder()
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .name(Property.ofValue(queue))
            .build()
            .run(runContextFactory.of());

        QueueBind.builder()
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .exchange(Property.ofValue(exchange))
            .queue(Property.ofValue(queue))
            .routingKey(Property.ofValue(routingKey))
            .build()
            .run(runContextFactory.of());

        var trigger = Trigger.builder()
            .id("watch-" + suffix)
            .type(Trigger.class.getName())
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .queue(Property.ofValue(queue))
            .consumerTag(Property.ofValue("KestraTriggerTest-" + suffix))
            .serdeType(Property.ofValue(SerdeType.STRING))
            .maxDuration(Property.ofValue(Duration.ofMillis(1)))
            .build();

        var triggerContext = TestsUtils.mockTrigger(runContextFactory, trigger);
        var firstEvaluationStartedAt = Instant.now();
        var first = trigger.evaluate(triggerContext.getKey(), triggerContext.getValue());
        var firstElapsed = Duration.between(firstEvaluationStartedAt, Instant.now());

        assertThat(first.isEmpty(), is(true));
        assertThat(firstElapsed.toMillis(), lessThan(400L));

        publishSingleMessage(exchange, routingKey, "value-window-" + suffix);

        var secondTrigger = Trigger.builder()
            .id("watch-next-" + suffix)
            .type(Trigger.class.getName())
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .queue(Property.ofValue(queue))
            .consumerTag(Property.ofValue("KestraTriggerTestNext-" + suffix))
            .serdeType(Property.ofValue(SerdeType.STRING))
            .maxRecords(Property.ofValue(1))
            .maxDuration(Property.ofValue(Duration.ofSeconds(2)))
            .build();

        var secondTriggerContext = TestsUtils.mockTrigger(runContextFactory, secondTrigger);
        var second = secondTrigger.evaluate(secondTriggerContext.getKey(), secondTriggerContext.getValue());
        assertThat(second.isPresent(), is(true));

        var count = (Integer) second.orElseThrow().getTrigger().getVariables().get("count");
        assertThat(count, is(1));
    }

    private void publishSingleMessage(String exchange, String routingKey, String payload) throws Exception {
        var task = Publish.builder()
            .id("publishTriggerBoundary")
            .type(Publish.class.getName())
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .exchange(Property.ofValue(exchange))
            .routingKey(Property.ofValue(routingKey))
            .from(JacksonMapper.toMap(Message.builder()
                .headers(ImmutableMap.of("testHeader", "KestraBoundaryTest"))
                .timestamp(Instant.now())
                .data(payload)
                .build()))
            .build();

        task.run(runContextFactory.of());
    }
}
