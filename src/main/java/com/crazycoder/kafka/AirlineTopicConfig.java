package com.crazycoder.kafka;


import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class AirlineTopicConfig {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        AdminClient adminClient = AdminClient.create(props);

        String topicName = "airline-bookings";

        NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
        newTopic.configs(Collections.singletonMap(TopicConfig.CLEANUP_POLICY_CONFIG, "compact,delete"));

        adminClient.createTopics(Collections.singletonList(newTopic));

        ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        Config newConfig = new Config(Arrays.asList(
                new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, "compact,delete"),
                new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.5"),
                new ConfigEntry(TopicConfig.DELETE_RETENTION_MS_CONFIG, "86400000"),
                new ConfigEntry(TopicConfig.SEGMENT_MS_CONFIG, "43200000")
        ));

        adminClient.incrementalAlterConfigs(Collections.singletonMap(topicResource,
                Arrays.asList(
                        new AlterConfigOp(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, "compact,delete"), AlterConfigOp.OpType.SET),
                        new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.5"), AlterConfigOp.OpType.SET),
                        new AlterConfigOp(new ConfigEntry(TopicConfig.DELETE_RETENTION_MS_CONFIG, "86400000"), AlterConfigOp.OpType.SET),
                        new AlterConfigOp(new ConfigEntry(TopicConfig.SEGMENT_MS_CONFIG, "43200000"), AlterConfigOp.OpType.SET)
                )
        ));

        adminClient.close();
    }
}
