package io.kestra.plugin.amqp;

import java.time.Instant;
import java.util.Arrays;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.amqp.models.Message;

import jakarta.inject.Inject;

@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractTriggerTest {
    @Inject
    protected RunContextFactory runContextFactory;

    @BeforeAll
    void setUp() throws Exception {
        DeclareExchange declareExchange = DeclareExchange.builder()
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .name(Property.ofValue("amqpTrigger.exchange"))
            .build();
        CreateQueue createQueue = CreateQueue.builder()
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .name(Property.ofValue("amqpTrigger.queue"))
            .build();
        QueueBind queueBind = QueueBind.builder()
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .exchange(Property.ofValue("amqpTrigger.exchange"))
            .queue(Property.ofValue("amqpTrigger.queue"))
            .build();

        declareExchange.run(runContextFactory.of());
        createQueue.run(runContextFactory.of());
        queueBind.run(runContextFactory.of());
    }

    protected Publish.Output publish() throws Exception {
        var task = Publish.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(Publish.class.getName())
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("5672"))
            .username(Property.ofValue("guest"))
            .password(Property.ofValue("guest"))
            .virtualHost(Property.ofValue("/my_vhost"))
            .exchange(Property.ofValue("amqpTrigger.exchange"))
            .from(
                Arrays.asList(
                    JacksonMapper.toMap(
                        Message.builder()
                            .headers(ImmutableMap.of("testHeader", "KestraTest"))
                            .timestamp(Instant.now())
                            .data("value-1")
                            .build()
                    ),
                    JacksonMapper.toMap(
                        Message.builder()
                            .appId("unit-kestra")
                            .timestamp(Instant.now())
                            .data("value-2")
                            .build()
                    )
                )
            )
            .build();

        return task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
    }
}
