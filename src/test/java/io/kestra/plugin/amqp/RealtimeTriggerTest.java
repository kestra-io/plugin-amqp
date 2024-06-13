package io.kestra.plugin.amqp;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.utils.TestsUtils;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.kestra.core.utils.Rethrow.throwRunnable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class RealtimeTriggerTest extends AbstractTriggerTest {
    @Test
    void flow() throws Exception {
        CountDownLatch queueCount = new CountDownLatch(4);

        Flux<Execution> receive = TestsUtils.receive(executionQueue, execution -> {
            queueCount.countDown();
            assertThat(execution.getLeft().getFlowId(), is("realtime"));
        });

        this.run("realtime.yaml", throwRunnable(() -> {
            publish();
            publish();

            boolean await = queueCount.await(1, TimeUnit.MINUTES);
            assertThat(await, is(true));
            List<Execution> executionList = receive.collectList().block();

            assertThat(executionList.size(), is(4));
            assertThat(executionList.stream().filter(execution -> execution.getTrigger().getVariables().get("data").equals("value-2")).count(), is(2L));
        }));
    }
}

