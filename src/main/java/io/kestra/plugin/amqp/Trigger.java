package io.kestra.plugin.amqp;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.ExecutionTrigger;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import javax.validation.constraints.NotNull;
import java.time.Duration;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
        title = "Wait for key list in Redis database"
)
@Plugin(
        examples = {
                @Example(
                        code = {
                                "id: watch",
                                "type: io.kestra.plugin.redis.Trigger",
                                "uri: redis://localhost:6379/0",
                                "key: mytriggerkey",
                                "count: 2"
                        }
                )
        }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Pull.Output> {
    @Schema(
            title = "The connection string"
    )
    @NotNull
    private String uri;


    @NotNull
    @PluginProperty(dynamic = true)
    private String queue;

    @Schema(
            title = "Acknowledge message(s)",
            description = "If the message should be acknowledge when consumed"
    )
    @Builder.Default
    private boolean acknowledge = true;

    @Builder.Default
    private String consumerTag = "Kestra";

    private Integer maxRecords;
    private Duration maxDuration;
    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        Pull task = Pull.builder()
                .uri(this.uri)
                .queue(this.queue)
                .acknowledge(this.acknowledge)
                .consumerTag(this.consumerTag)
                .maxDuration(this.maxDuration)
                .maxRecords(this.maxRecords)
                .build();

        Pull.Output run = task.run(runContext);

        if (logger.isDebugEnabled()) {
            logger.debug("Consumed '{}' messaged.", run.getCount());
        }

        if (run.getCount() == 0) {
            return Optional.empty();
        }

        String executionId = IdUtils.create();

        ExecutionTrigger executionTrigger = ExecutionTrigger.of(
                this,
                run
        );

        Execution execution = Execution.builder()
                .id(executionId)
                .namespace(context.getNamespace())
                .flowId(context.getFlowId())
                .flowRevision(context.getFlowRevision())
                .state(new State())
                .trigger(executionTrigger)
                .build();

        return Optional.of(execution);
    }

    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

}
