package io.kestra.plugin.amqp;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.plugin.amqp.models.SerdeType;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Poll an AMQP 1.0 address into batch executions",
    description = "Polls the address every 60 seconds by default, consumes like `io.kestra.plugin.amqp.Amqp1Consume` until `maxRecords`, `maxDuration` or `pollDuration` is reached, and launches one execution per non-empty batch with payloads stored at `trigger.uri`. Set at least one of `maxRecords` or `maxDuration`."
)
@Plugin(
    examples = {
        @Example(
            title = "Start an execution for each batch of up to 100 messages received on an Azure Service Bus queue.",
            full = true,
            code = """
                id: azure_service_bus_trigger
                namespace: company.team

                tasks:
                  - id: log
                    type: io.kestra.plugin.core.log.Log
                    message: "Received {{ trigger.count }} messages stored at {{ trigger.uri }}"

                triggers:
                  - id: trigger
                    type: io.kestra.plugin.amqp.Amqp1Trigger
                    host: my-namespace.servicebus.windows.net
                    ssl: true
                    username: RootManageSharedAccessKey
                    password: "{{ secret('SERVICE_BUS_SAS_KEY') }}"
                    address: orders
                    maxRecords: 100
                """
        )
    }
)
public class Amqp1Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Amqp1Consume.Output>, Amqp1ConsumeInterface, Amqp1ConnectionInterface {
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @NotNull
    private Property<String> host;

    private Property<Integer> port;

    private Property<String> username;

    @PluginProperty(secret = true, group = "connection")
    @ToString.Exclude
    private Property<String> password;

    @Builder.Default
    private Property<Boolean> ssl = Property.ofValue(false);

    @Builder.Default
    private Property<Duration> connectionTimeout = Property.ofValue(DEFAULT_CONNECTION_TIMEOUT);

    private Property<String> address;

    @Builder.Default
    private Property<SerdeType> serdeType = Property.ofValue(SerdeType.STRING);

    @Builder.Default
    private Property<Boolean> autoAck = Property.ofValue(false);

    private Property<Integer> maxRecords;

    private Property<Duration> maxDuration;

    @Builder.Default
    private Property<Duration> pollDuration = Property.ofValue(DEFAULT_POLL_DURATION);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicReference<Amqp1Consume> currentTask = new AtomicReference<>();

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        if (!this.isActive.get()) {
            return Optional.empty();
        }

        var runContext = conditionContext.getRunContext();
        var task = Amqp1Consume.builder()
            .id(this.id)
            .type(Amqp1Consume.class.getName())
            .host(this.host)
            .port(this.port)
            .username(this.username)
            .password(this.password)
            .ssl(this.ssl)
            .connectionTimeout(this.connectionTimeout)
            .address(this.address)
            .serdeType(this.serdeType)
            .autoAck(this.autoAck)
            .maxRecords(this.maxRecords)
            .maxDuration(this.maxDuration)
            .pollDuration(this.pollDuration)
            .build();

        this.currentTask.set(task);

        Amqp1Consume.Output run;
        try {
            // a kill()/stop() landing before currentTask was published would otherwise be missed for this poll
            if (!this.isActive.get()) {
                return Optional.empty();
            }

            run = task.run(runContext);
        } finally {
            this.currentTask.set(null);
        }

        // a killed consume returns normally with a partial, unaccepted batch: never emit it
        if (!this.isActive.get()) {
            return Optional.empty();
        }

        runContext.logger().debug("Consumed {} messages", run.getCount());

        if (run.getCount() == 0) {
            return Optional.empty();
        }

        return Optional.of(TriggerService.generateExecution(this, conditionContext, context, run));
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void kill() {
        this.stop();
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void stop() {
        if (!this.isActive.compareAndSet(true, false)) {
            return;
        }

        var task = this.currentTask.get();
        if (task != null) {
            task.kill();
        }
    }
}
