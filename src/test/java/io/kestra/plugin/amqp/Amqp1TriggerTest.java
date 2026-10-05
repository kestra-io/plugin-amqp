package io.kestra.plugin.amqp;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

class Amqp1TriggerTest extends AbstractAmqp1Test {
    @Test
    void shouldStartAnExecutionForEachBatch() throws Exception {
        var address = randomAddress();
        this.publish(address, List.of(Map.of("data", "value-1"), Map.of("data", "value-2")));

        var trigger = this.trigger(address);
        var context = TestsUtils.mockTrigger(this.runContextFactory, trigger);
        var execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));
        var variables = execution.get().getTrigger().getVariables();
        assertThat(variables.get("count"), is(2));
        assertThat(variables.get("uri"), is(notNullValue()));
    }

    @Test
    void shouldNotStartAnExecutionWhenNoMessageIsAvailable() throws Exception {
        var trigger = this.trigger(randomAddress());
        var context = TestsUtils.mockTrigger(this.runContextFactory, trigger);

        assertThat(trigger.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));
    }

    @Test
    void shouldNotConsumeOnceStopped() throws Exception {
        var address = randomAddress();
        this.publish(address, Map.of("data", "value-1"));

        var trigger = this.trigger(address);
        trigger.stop();
        var context = TestsUtils.mockTrigger(this.runContextFactory, trigger);

        assertThat(trigger.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));
        assertThat(this.consume(this.consumeTask(address).maxRecords(Property.ofValue(1)).build()).getCount(), is(1));
    }

    private Amqp1Trigger trigger(String address) {
        return Amqp1Trigger.builder()
            .id("trigger-" + IdUtils.create())
            .type(Amqp1Trigger.class.getName())
            .host(Property.ofValue(HOST))
            .port(Property.ofValue(PORT))
            .username(Property.ofValue(USERNAME))
            .password(Property.ofValue(PASSWORD))
            .address(Property.ofValue(address))
            .maxRecords(Property.ofValue(10))
            .pollDuration(Property.ofValue(Duration.ofSeconds(1)))
            .build();
    }
}
