package io.kestra.plugin.amqp;

import io.kestra.core.models.executions.Execution;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.kestra.core.utils.Rethrow.throwRunnable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class RealtimeTriggerTest extends AbstractTriggerTest {
    @Test
    void flow() throws Exception {
        CountDownLatch queueCount = new CountDownLatch(4);
        List<Execution> executionList = new CopyOnWriteArrayList<>();

        executionQueue.receive(RealtimeTriggerTest.class, execution -> {
            executionList.add(execution.getLeft());

            queueCount.countDown();
            assertThat(execution.getLeft().getFlowId(), is("realtime"));
        });

        this.run("realtime.yaml", throwRunnable(() -> {
            publish();
            publish();

            queueCount.await(1, TimeUnit.MINUTES);

            assertThat(executionList.size(), is(4));
            assertThat(executionList.stream().filter(execution -> execution.getTrigger().getVariables().get("data").equals("value-2")).count(), is(2L));
        }));
    }
}

