package io.kestra.plugin.amqp;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.utils.TestsUtils;

import reactor.core.publisher.Flux;

import static io.kestra.core.utils.Rethrow.throwRunnable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class RealtimeTriggerTest extends AbstractTriggerTest {
    @Test
    void flow() throws Exception {
        // the execution queue emits one message per execution state change, so both the latch and the
        // assertions are based on distinct execution identifiers instead of the raw message count
        Set<String> executionIds = ConcurrentHashMap.newKeySet();
        CountDownLatch queueCount = new CountDownLatch(4);

        Flux<Execution> receive = TestsUtils.receive(executionQueue, either ->
        {
            if (either.isRight()) {
                return;
            }

            Execution execution = either.getLeft();
            assertThat(execution.getFlowId(), is("realtime"));

            if (executionIds.add(execution.getId())) {
                queueCount.countDown();
            }
        });

        this.run("realtime.yaml", throwRunnable(() ->
        {
            publish();
            publish();

            boolean await = queueCount.await(1, TimeUnit.MINUTES);
            assertThat(await, is(true));
            Collection<Execution> executions = distinctExecutions(receive);

            assertThat(executions.size(), is(4));
            assertThat(executions.stream().filter(execution -> execution.getTrigger().getVariables().get("data").equals("value-2")).count(), is(2L));
        }));
    }

    /**
     * Collects the buffered execution messages, keeping the latest message of each execution.
     */
    private Collection<Execution> distinctExecutions(Flux<Execution> receive) {
        return receive.collectList().block()
            .stream()
            .collect(Collectors.toMap(Execution::getId, execution -> execution, (first, last) -> last, LinkedHashMap::new))
            .values();
    }
}
