// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.config.kafka;

import de.telekom.eni.pandora.horizon.kafka.config.KafkaProperties;
import de.telekom.eni.pandora.horizon.model.meta.EventRetentionTime;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configuration class for setting up Kafka topics.
 * It provides Spring beans for Kafka administration and configures new topics
 * with different retention time based on {@link EventRetentionTime} enumeration values.
 */
@Configuration
public class KafkaTopicConfig {

    /**
     * KafkaAdmin bean configured with bootstrap servers information
     * from {@link KafkaProperties}.
     *
     * @param props KafkaProperties which includes bootstrap servers configuration.
     * @return the configured KafkaAdmin Spring bean.
     */
    @Bean
    public KafkaAdmin kafkaAdmin(KafkaProperties props) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, props.getBootstrapServers());
        return new KafkaAdmin(configs);
    }

    /**
     * A configuration bean for creating Kafka topics according to
     * the {@link EventRetentionTime} enumeration. Each individual value in
     * {@link EventRetentionTime} defines a separate topic.
     *
     * @return {@link KafkaAdmin.NewTopics} instance encapsulating all new topics configuration.
     */
    @Bean
    public KafkaAdmin.NewTopics publishedMessages() {
        var eventRetentionTimes = EventRetentionTime.values();
        List<NewTopic> topics = new ArrayList<>();
        for (var eventRetentionTime : eventRetentionTimes) {
            topics.add(
                    TopicBuilder
                            .name(eventRetentionTime.getTopic())
                            .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(eventRetentionTime.getRetentionInMs()))
                            .build()); // maybe we also need to set replicates and partitions (just use getter topic)
        }

        topics = topics.stream().distinct().toList();

        return new KafkaAdmin.NewTopics(topics.toArray(new NewTopic[0])); // <-- pass list as varargs
    }
}
