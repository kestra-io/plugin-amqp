package io.kestra.plugin.amqp;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.PemReader;

final class AmqpTls {
    private AmqpTls() {
    }

    static void configure(ConnectionFactory factory, String caCertificatePem) throws GeneralSecurityException, IOException {
        if (caCertificatePem == null) {
            factory.useSslProtocol();
            return;
        }
        // This overload also turns on hostname verification.
        factory.useSslProtocol(createSslContext(caCertificatePem));
    }

    /**
     * Builds an {@link SSLContext} that trusts only the given PEM CA (or chain).
     * Fail closed for a blank or empty PEM - usually a secret that rendered to nothing.
     */
    static SSLContext createSslContext(String caCertificatePem) throws GeneralSecurityException, IOException {
        if (caCertificatePem == null || caCertificatePem.isBlank()) {
            throw new IllegalArgumentException("`sslCaCertificate` is set but empty; check the secret or expression it uses");
        }

        var certificates = PemReader.readCertificateChain(caCertificatePem);
        if (certificates.isEmpty()) {
            throw new IllegalArgumentException("`sslCaCertificate` must contain at least one PEM certificate (-----BEGIN CERTIFICATE-----)");
        }

        var trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        for (var i = 0; i < certificates.size(); i++) {
            trustStore.setCertificateEntry("amqp-ca-" + i, certificates.get(i));
        }

        var trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);

        var sslContext = SSLContext.getInstance(
            ConnectionFactory.computeDefaultTlsProtocol(SSLContext.getDefault().getSupportedSSLParameters().getProtocols())
        );
        sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
        return sslContext;
    }
}
