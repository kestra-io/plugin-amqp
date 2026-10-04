package io.kestra.plugin.amqp;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.List;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.PemReader;

/**
 * TLS setup for AMQP connections. Server certificate and hostname verification are always on.
 */
final class AmqpTls {
    private AmqpTls() {
    }

    /**
     * Enables TLS on the factory. Without a CA certificate the JVM's default trust store is used;
     * with one, only brokers whose certificate chains to that CA are trusted.
     */
    static void configure(ConnectionFactory factory, String caCertificatePem) throws GeneralSecurityException, IOException {
        if (caCertificatePem == null || caCertificatePem.isBlank()) {
            factory.useSslProtocol();
            return;
        }

        List<X509Certificate> certificates = PemReader.readCertificateChain(caCertificatePem);
        if (certificates.isEmpty()) {
            throw new IllegalArgumentException("`sslCaCertificate` must contain at least one PEM certificate (-----BEGIN CERTIFICATE-----)");
        }

        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        for (int i = 0; i < certificates.size(); i++) {
            trustStore.setCertificateEntry("amqp-ca-" + i, certificates.get(i));
        }

        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);

        SSLContext sslContext = SSLContext.getInstance(
            ConnectionFactory.computeDefaultTlsProtocol(SSLContext.getDefault().getSupportedSSLParameters().getProtocols())
        );
        sslContext.init(null, trustManagerFactory.getTrustManagers(), null);

        // useSslProtocol(SSLContext) also enables hostname verification.
        factory.useSslProtocol(sslContext);
    }
}
