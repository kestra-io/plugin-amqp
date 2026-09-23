package io.kestra.plugin.amqp;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import com.rabbitmq.client.Connection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;

/**
 * amqp-client 5.x's {@code Connection.abort(int)} still runs a synchronous, bounded close handshake
 * internally (a blocking {@code getReply(timeoutMs)}) before forcibly closing the socket — see review
 * thread PRRT_kwDOItooUc6k22ns. {@link Consume.ConsumeThread#abort()} is called from the worker's
 * kill/stop thread and must never block on it. A JDK dynamic proxy stands in for a {@link Connection}
 * whose {@code abort()} blocks, so no broker or extra test dependency is needed to prove it.
 */
class ConsumeThreadAbortTest {
    @Test
    void abortShouldReturnPromptlyEvenIfTheUnderlyingConnectionAbortBlocks() throws Exception {
        var blockingAbortConnection = (Connection) Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class[]{Connection.class},
            (proxy, method, args) -> {
                if ("abort".equals(method.getName())) {
                    Thread.sleep(3000);
                }
                return null;
            }
        );

        var thread = new Consume.ConsumeThread(
            null,
            null,
            null,
            false,
            message -> {},
            () -> true
        );

        var connectionField = Consume.ConsumeThread.class.getDeclaredField("connection");
        connectionField.setAccessible(true);
        connectionField.set(thread, blockingAbortConnection);

        var startedAt = Instant.now();
        thread.abort();
        var elapsed = Duration.between(startedAt, Instant.now());

        assertThat(elapsed.toMillis(), lessThan(200L));
    }
}
