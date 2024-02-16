// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.config.kafka;

import de.telekom.eni.pandora.horizon.kafka.config.KafkaProperties;
import de.telekom.eni.pandora.horizon.tracing.HorizonTracer;
import de.telekom.horizon.galaxy.config.GalaxyConfig;
import de.telekom.horizon.galaxy.kafka.PublishedMessageListener;
import de.telekom.horizon.galaxy.kafka.PublishedMessageTaskFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

@Configuration
@Slf4j
public class KafkaConsumerConfig {

    /**
     * This class handles the configuration of the Kafka concurrent message listener container.
     */
    @Autowired
    @DependsOn(value = {
            "hazelcastInstance"
    })
    @Bean
    public ConcurrentMessageListenerContainer<String, String> concurrentMessageListenerContainer(PublishedMessageTaskFactory publishedMessageTaskFactory,
                                                                                                 KafkaProperties props,
                                                                                                 ConsumerFactory<String, String> consumerFactory,
                                                                                                 GalaxyConfig galaxyConfig,
                                                                                                 HorizonTracer horizonTracer) {
        var containerProperties = new ContainerProperties(galaxyConfig.getConsumingTopic());
        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL);
        containerProperties.setMessageListener(new PublishedMessageListener(publishedMessageTaskFactory, horizonTracer, galaxyConfig));
        ConcurrentMessageListenerContainer<String, String> listenerContainer = new ConcurrentMessageListenerContainer<>(consumerFactory, containerProperties);
        listenerContainer.setAutoStartup(false);
        listenerContainer.setConcurrency(props.getPartitionCount());
        // bean name is the prefix of kafka consumer thread name
        listenerContainer.setBeanName("kafka-message-listener");
        return listenerContainer;
    }

}
