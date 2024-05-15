package io.kestra.plugin.amqp;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.*;
import io.kestra.plugin.amqp.models.Message;
import io.kestra.plugin.amqp.models.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.time.Duration;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "React to and consume messages from an AMQP queue creating one executions for each message."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "url: amqp://guest:guest@localhost:5672/my_vhost",
                "maxRecords: 2",
                "queue: amqpTrigger.queue"
            }
        )
    },
    beta = true
)
public class RealtimeTrigger extends AbstractTrigger implements RealtimeTriggerInterface, TriggerOutput<Message>, ConsumeBaseInterface, AmqpConnectionInterface {
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    private String url;
    private String host;
    private String port;
    private String username;
    private String password;
    private String virtualHost;

    private String queue;

    @Builder.Default
    private String consumerTag = "Kestra";

    private Integer maxRecords;

    private Duration maxDuration;

    @Builder.Default
    private SerdeType serdeType = SerdeType.STRING;

    @Override
    public Publisher<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
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

        return Flux.from(task.stream(conditionContext.getRunContext()))
            .map((record) -> TriggerService.generateRealtimeExecution(this, context, record));
    }
}
