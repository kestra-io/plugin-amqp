package io.kestra.plugin.amqp;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.plugin.amqp.models.SerdeType;

import jakarta.validation.constraints.NotNull;
import io.kestra.core.models.annotations.PluginProperty;
import java.time.Duration;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractAmqpTrigger extends AbstractTrigger implements AmqpConnectionInterface, ConsumeBaseInterface {

    @Deprecated
    private Property<String> url;

    @NotNull
    private Property<String> host;

    private Property<String> port;

    private Property<String> username;

    @PluginProperty(secret = true, group = "connection")
    @ToString.Exclude
    private Property<String> password;

    @Builder.Default
    private Property<String> virtualHost = Property.ofValue("/");

    private Property<Boolean> ssl;

    private Property<String> sslCaCertificate;

    private Property<String> queue;

    @Builder.Default
    private Property<String> consumerTag = Property.ofValue("Kestra");

    @Builder.Default
    private Property<SerdeType> serdeType = Property.ofValue(SerdeType.STRING);

    /** Consume task with the connection and consumer settings shared by both triggers. */
    protected Consume consumeTask(Property<Boolean> autoAck, Property<Integer> maxRecords, Property<Duration> maxDuration) {
        return Consume.builder()
            .url(url)
            .host(host)
            .port(port)
            .username(username)
            .password(password)
            .virtualHost(virtualHost)
            .ssl(ssl)
            .sslCaCertificate(sslCaCertificate)
            .queue(queue)
            .consumerTag(consumerTag)
            .serdeType(serdeType)
            .autoAck(autoAck)
            .maxRecords(maxRecords)
            .maxDuration(maxDuration)
            .build();
    }
}
