package io.kestra.plugin.amqp;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.schedulers.AbstractScheduler;
import io.kestra.core.schedulers.DefaultScheduler;
import io.kestra.core.schedulers.SchedulerExecutionStateInterface;
import io.kestra.core.schedulers.SchedulerTriggerStateInterface;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.context.ApplicationContext;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import jdk.jfr.Name;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TriggerTest {
    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private SchedulerTriggerStateInterface triggerState;

    @Inject
    SchedulerExecutionStateInterface executionState;

    @Inject
    private FlowListeners flowListenersService;

    @Inject
    @Name(QueueFactoryInterface.EXECUTION_NAMED)
    private QueueInterface<Execution> executionQueue;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void flow() throws Exception {
        CountDownLatch queueCount = new CountDownLatch(1);

        try (AbstractScheduler scheduler = new DefaultScheduler(
                this.applicationContext,
                this.flowListenersService,
                this.executionState,
                this.triggerState
        )) {
            AtomicReference<Execution> last = new AtomicReference<>();

            executionQueue.receive(TriggerTest.class, execution -> {
                last.set(execution);

                queueCount.countDown();
                assertThat(execution.getFlowId(), is("trigger"));
            });
            Publish task = Publish.builder()
                    .id(TriggerTest.class.getSimpleName())
                    .type(Publish.class.getName())
                    .uri("amqp://guest:guest@localhost:5672")
                    .exchange("amqpTrigger.exchange")
                    .routingKey("")
                    .headers(ImmutableMap.of("testHeader", "KestraTest"))
                    .from(Arrays.asList(new String[]{"value-1", "value-2"}))
                    .build();

            scheduler.run();

            repositoryLoader.load(Objects.requireNonNull(TriggerTest.class.getClassLoader().getResource("flows")));

            task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

            queueCount.await(1, TimeUnit.MINUTES);

            Integer trigger = (Integer) last.get().getTrigger().getVariables().get("count");

            assertThat(trigger, greaterThanOrEqualTo(2));
        }
    }

    @BeforeAll
    void setUp() throws Exception {

        CreateExchange createExchange = CreateExchange.builder()
                .uri("amqp://guest:guest@localhost:5672/")
                .name("amqpTrigger.exchange")
                .build();
        CreateQueue createQueue = CreateQueue.builder()
                .uri("amqp://guest:guest@localhost:5672/")
                .name("amqpTrigger.queue")
                .build();
        QueueBind queueBind = QueueBind.builder()
                .uri("amqp://guest:guest@localhost:5672/")
                .exchange("amqpTrigger.exchange")
                .queue("amqpTrigger.queue")
                .build();

        createExchange.run(runContextFactory.of());
        createQueue.run(runContextFactory.of());
        queueBind.run(runContextFactory.of());
    }


}

