package io.kestra.plugin.amqp;

import io.kestra.core.models.executions.Execution;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.kestra.core.utils.Rethrow.throwRunnable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

class TriggerTest extends AbstractTriggerTest {
    @Test
    void flow() throws Exception {
        CountDownLatch queueCount = new CountDownLatch(1);
        AtomicReference<Execution> last = new AtomicReference<>();

        executionQueue.receive(TriggerTest.class, execution -> {
            last.set(execution.getLeft());

            queueCount.countDown();
            assertThat(execution.getLeft().getFlowId(), is("trigger"));
        });

        this.run("trigger.yaml", throwRunnable(() -> {
            publish();

            queueCount.await(1, TimeUnit.MINUTES);

            Integer trigger = (Integer) last.get().getTrigger().getVariables().get("count");

            assertThat(trigger, greaterThanOrEqualTo(2));
        }));
    }
}

