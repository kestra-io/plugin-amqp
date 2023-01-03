package io.kestra.plugin.amqp;

import com.rabbitmq.client.*;

public class Tools {

    /** Create a factory from a user:pass@host:port/vhost URI */
    public static ConnectionFactory getAMQPFactory (String uri) {
        String user = "guest";
        String pass = "guest";
        String host = "localhost";
        String port = "5672";
        String vhost = "/";

        if (uri != null && !uri.trim().equals("")) {
            String uri_process = uri.trim();
            if (uri_process.startsWith("amqp://")) {
                uri_process = uri_process.substring(7);
            }
            while (uri_process.endsWith("/")) {
                uri_process = uri_process.substring(0, uri_process.length()-1);
            }
            // check if exists a user/pass part
            if (uri_process.contains("@")) {
                String part = uri_process.substring(0, uri_process.indexOf('@'));
                int pos = part.indexOf(':');
                if (pos > 0) {
                    user = part.substring(0, pos);
                    pass = part.substring(pos+1);
                } else {
                    user = part;
                    pass = "";
                }
                uri_process = uri_process.substring(uri_process.indexOf('@')+1);
            }

            // check vhost part
            if (uri_process.contains("/")) {
                vhost = uri_process.substring(uri_process.indexOf('/')+1);
                uri_process = uri_process.substring(0, uri_process.indexOf('/'));
            }

            // host part
            if (uri_process.contains(":")) {
                int pos = uri_process.indexOf(':');
                host = uri_process.substring(0, pos);
                port = uri_process.substring(pos+1);
            }
        }

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(Integer.parseInt(port));
        factory.setUsername(user);
        factory.setPassword(pass);
        factory.setVirtualHost(vhost);

        return factory;
    }
}
