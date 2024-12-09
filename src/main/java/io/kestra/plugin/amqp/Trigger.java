package io.kestra.plugin.amqp;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.amqp.models.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Consume messages periodically from a AMQP queue and create one execution per batch.",
    description = "Note that you don't need an extra task to consume the message from the event trigger. The trigger will automatically consume messages and you can retrieve their content in your flow using the `{{ trigger.uri }}` variable. If you would like to consume each message from a AMQP queue in real-time and create one execution per message, you can use the [io.kestra.plugin.amqp.RealtimeTrigger](https://kestra.io/plugins/plugin-amqp/triggers/io.kestra.plugin.amqp.realtimetrigger) instead."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: amqp_trigger
                namespace: company.team

                tasks:
                  - id: trigger
                    type: io.kestra.plugin.amqp.Trigger
                    url: amqp://guest:guest@localhost:5672/my_vhost
                    maxRecords: 2
                    queue: amqpTrigger.queue
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Consume.Output>, ConsumeInterface, AmqpConnectionInterface {
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    private Property<String> url;
    private Property<String> host;
    private Property<String> port;
    private Property<String> username;
    private Property<String> password;
    private Property<String> virtualHost;

    private Property<String> queue;

    @Builder.Default
    private Property<String> consumerTag = Property.of("Kestra");

    private Property<Integer> maxRecords;

    private Property<Duration> maxDuration;

    @Builder.Default
    private Property<SerdeType> serdeType = Property.of(SerdeType.STRING);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        Consume task = Consume.builder()
            .url(this.url)
            .host(this.host)
            .port(this.port)
            .username(this.username)
            .password(this.password)
            .virtualHost(this.virtualHost)
            .queue(this.queue)
            .consumerTag(this.consumerTag)
            .maxRecords(this.maxRecords)
            .maxDuration(this.maxDuration)
            .serdeType(this.serdeType)
            .build();

        Consume.Output run = task.run(runContext);

        if (logger.isDebugEnabled()) {
            logger.debug("Consumed '{}' messaged.", run.getCount());
        }

        if (run.getCount() == 0) {
            return Optional.empty();
        }

        Execution execution = TriggerService.generateExecution(this, conditionContext, context, run);

        return Optional.of(execution);
    }
}
