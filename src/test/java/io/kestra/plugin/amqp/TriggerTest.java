package io.kestra.plugin.amqp;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.utils.TestsUtils;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.kestra.core.utils.Rethrow.throwRunnable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

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
}

