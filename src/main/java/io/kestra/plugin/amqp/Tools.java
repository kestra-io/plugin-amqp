package io.kestra.plugin.amqp;

import com.rabbitmq.client.*;

import java.net.URI;
import java.net.URISyntaxException;

public class Tools {

    /** Create a factory from a user:pass@host:port/vhost URI */
    public static ConnectionFactory getAMQPFactory (String uri) throws URISyntaxException {
        String user;
        String pass;

        URI amqpUri = new URI(uri);
        String auth = amqpUri.getUserInfo();
        int pos = auth.indexOf(':');
        if (pos > 0) {
            user = auth.substring(0, pos);
            pass = auth.substring(pos+1);
        } else {
            user = auth;
            pass = "";
        }
        String host = amqpUri.getHost();
        Integer port = amqpUri.getPort();
        String  vhost = amqpUri.getPath();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(user);
        factory.setPassword(pass);
        factory.setVirtualHost(vhost);

        return factory;
    }
}
