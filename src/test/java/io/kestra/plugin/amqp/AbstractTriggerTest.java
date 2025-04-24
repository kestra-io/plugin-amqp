package io.kestra.plugin.amqp;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.runners.Worker;
import io.kestra.core.schedulers.AbstractScheduler;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.jdbc.runner.JdbcScheduler;
import io.kestra.plugin.amqp.models.Message;
import io.micronaut.context.ApplicationContext;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractTriggerTest {
    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private FlowListeners flowListenersService;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @Inject
    protected RunContextFactory runContextFactory;

    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    protected QueueInterface<Execution> executionQueue;

    @BeforeAll
    void setUp() throws Exception {
        DeclareExchange declareExchange = DeclareExchange.builder()
            .url(Property.of("amqp://guest:guest@localhost:5672/my_vhost"))
            .name(Property.of("amqpTrigger.exchange"))
            .build();
        CreateQueue createQueue = CreateQueue.builder()
            .url(Property.of("amqp://guest:guest@localhost:5672/my_vhost"))
            .name(Property.of("amqpTrigger.queue"))
            .build();
        QueueBind queueBind = QueueBind.builder()
            .url(Property.of("amqp://guest:guest@localhost:5672/my_vhost"))
            .exchange(Property.of("amqpTrigger.exchange"))
            .queue(Property.of("amqpTrigger.queue"))
            .build();

        declareExchange.run(runContextFactory.of());
        createQueue.run(runContextFactory.of());
        queueBind.run(runContextFactory.of());
    }

    protected void run(String filename, Runnable runnable) throws IOException, URISyntaxException {
        try (
            AbstractScheduler scheduler = new JdbcScheduler(this.applicationContext, this.flowListenersService);
            Worker worker = applicationContext.createBean(Worker.class, IdUtils.create(), 8, null);
        ) {
            worker.run();
            scheduler.run();

            repositoryLoader.load("null", Objects.requireNonNull(TriggerTest.class.getClassLoader().getResource("flows/" + filename)));

            runnable.run();
        }
    }

    protected Publish.Output publish() throws Exception {
        var task = Publish.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(Publish.class.getName())
            .url(Property.of("amqp://guest:guest@localhost:5672/my_vhost"))
            .exchange(Property.of("amqpTrigger.exchange"))
            .from(Arrays.asList(
                JacksonMapper.toMap(Message.builder()
                    .headers(ImmutableMap.of("testHeader", "KestraTest"))
                    .timestamp(Instant.now())
                    .data("value-1")
                    .build()),
                JacksonMapper.toMap(Message.builder()
                    .appId("unit-kestra")
                    .timestamp(Instant.now())
                    .data("value-2")
                    .build())
            ))
            .build();

        return task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
    }
}

