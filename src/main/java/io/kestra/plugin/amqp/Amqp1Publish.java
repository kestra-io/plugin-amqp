package io.kestra.plugin.amqp;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.exceptions.ClientException;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.amqp.models.Amqp1Message;
import io.kestra.plugin.amqp.models.SerdeType;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Publish messages to an AMQP 1.0 address",
    description = "Sends one or more messages to a queue or topic over AMQP 1.0, serializing the payload according to `serdeType` and waiting for the broker to accept each message. Works with Azure Service Bus, ActiveMQ Artemis, RabbitMQ 4 and other AMQP 1.0 brokers; use `io.kestra.plugin.amqp.Publish` for AMQP 0.9.1."
)
@Plugin(
    examples = {
        @Example(
            title = "Publish messages to an ActiveMQ Artemis queue.",
            full = true,
            code = """
                id: amqp1_publish
                namespace: company.team

                tasks:
                  - id: publish
                    type: io.kestra.plugin.amqp.Amqp1Publish
                    host: localhost
                    port: 5672
                    username: artemis
                    password: "{{ secret('AMQP_PASSWORD') }}"
                    address: orders
                    from:
                      - data: value-1
                        applicationProperties:
                          source: kestra
                      - data: value-2
                        durable: true
                """
        ),
        @Example(
            title = "Publish JSON messages to an Azure Service Bus queue, authenticating with a shared access policy.",
            full = true,
            code = """
                id: azure_service_bus_publish
                namespace: company.team

                tasks:
                  - id: publish
                    type: io.kestra.plugin.amqp.Amqp1Publish
                    host: my-namespace.servicebus.windows.net
                    ssl: true
                    username: RootManageSharedAccessKey
                    password: "{{ secret('SERVICE_BUS_SAS_KEY') }}"
                    address: orders
                    serdeType: JSON
                    from:
                      - data:
                          orderId: 42
                          status: CREATED
                        contentType: application/json
                        messageId: order-42
                        subject: order-created
                        applicationProperties:
                          region: eu-west
                """
        )
    },
    metrics = {
        @Metric(
            name = "published.records",
            type = Counter.TYPE,
            unit = "records",
            description = "Number of messages published to the AMQP 1.0 address"
        )
    }
)
public class Amqp1Publish extends AbstractAmqp1Connection implements RunnableTask<Amqp1Publish.Output>, Data.From {
    @NotNull
    @Schema(
        title = "Target address",
        description = """
            Address to send to, in the broker's format: the queue or topic name for ActiveMQ Artemis and Azure Service Bus, \
            or `/queues/<queue>` and `/exchanges/<exchange>/<routing-key>` for RabbitMQ 4."""
    )
    @PluginProperty(group = "main")
    private Property<String> address;

    @NotNull
    @Schema(
        title = Data.From.TITLE,
        description = Data.From.DESCRIPTION,
        anyOf = { String.class, Amqp1Message[].class, Amqp1Message.class }
    )
    @PluginProperty(group = "main")
    private Object from;

    @Builder.Default
    @Schema(title = "Serializer used for the message payload", description = "Defaults to STRING.")
    @PluginProperty(group = "advanced")
    private Property<SerdeType> serdeType = Property.ofValue(SerdeType.STRING);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rAddress = runContext.render(this.address).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("`address` is required to publish messages"));
        var rSerdeType = runContext.render(this.serdeType).as(SerdeType.class).orElse(SerdeType.STRING);
        var rConnectionTimeout = runContext.render(this.getConnectionTimeout()).as(Duration.class).orElse(DEFAULT_CONNECTION_TIMEOUT);

        try (
            var client = Client.create();
            var connection = this.connect(runContext, client);
            var sender = connection.openSender(rAddress)
        ) {
            var count = Data.from(this.from)
                .readAs(runContext, Amqp1Message.class, msg -> JacksonMapper.toMap(msg, Amqp1Message.class))
                .map(throwFunction(message ->
                {
                    this.send(sender, message, rSerdeType, rConnectionTimeout);
                    return 1;
                }))
                .reduce(Integer::sum)
                .blockOptional()
                .orElse(0);

            runContext.metric(Counter.of("published.records", count, "address", rAddress));

            return Output.builder()
                .messagesCount(count)
                .build();
        }
    }

    private void send(Sender sender, Amqp1Message message, SerdeType serdeType, Duration timeout) throws ClientException, IOException {
        sender.send(message.toAmqpMessage(serdeType.serialize(message.getData())))
            .awaitAccepted(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Number of messages published"
        )
        private final Integer messagesCount;
    }
}
