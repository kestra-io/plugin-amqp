@PluginSubGroup(
    description = "Tasks that connect to RabbitMQ brokers to declare exchanges and queues, bind routing keys, publish messages, and consume them via tasks or triggers (batch or real time). Provide host/port/credentials/virtual host values for each connection, and set stop conditions such as maxDuration or maxRecords when consuming.",
    categories = {
        PluginSubGroup.PluginCategory.DATA,
        PluginSubGroup.PluginCategory.INFRASTRUCTURE
    }
)
package io.kestra.plugin.amqp;

import io.kestra.core.models.annotations.PluginSubGroup;
