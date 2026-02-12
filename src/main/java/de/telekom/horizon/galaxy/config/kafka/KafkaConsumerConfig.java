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
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.InterruptException;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;

@Configuration
@Slf4j
public class KafkaConsumerConfig {

    private final ApplicationContext applicationContext;

    public KafkaConsumerConfig(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    private boolean isFatalException(Throwable exception) {
        if (exception == null) {
            return false;
        }
        if (exception instanceof InterruptedException ||
            exception instanceof InterruptException ||
            exception instanceof AuthenticationException ||
            exception instanceof AuthorizationException ||
            exception instanceof FencedInstanceIdException ||
            exception instanceof IllegalStateException) {
            return true;
        }
        return isFatalException(exception.getCause());
    }

    /**
     * Creates a custom error handler that shuts down the application when a fatal exception occurs
     * during Kafka poll(). This prevents the application from being stuck in an unhealthy state
     * when unrecoverable errors occur outside of record processing.
     * Fatal exceptions include:
     * - InterruptException / InterruptedException: Thread interrupted during poll
     * - AuthenticationException: SASL/SSL authentication failed
     * - AuthorizationException: No permission to access topic/group
     * - FencedInstanceIdException: Static member fenced by another instance
     * - IllegalStateException: Consumer in invalid state
     * Record-level exceptions use the default behavior (retry + log + skip).
     *
     * @param healthIndicator Health indicator to mark as unhealthy on fatal exceptions
     * @return CommonErrorHandler that handles fatal exceptions during poll() by stopping the application
     */
    @Bean
    public CommonErrorHandler kafkaErrorHandler(KafkaConsumerHealthIndicator healthIndicator) {
        return new DefaultErrorHandler() {
            @Override
            public void handleOtherException(@NotNull Exception exception, @NotNull org.apache.kafka.clients.consumer.Consumer<?, ?> consumer,
                                             @NotNull MessageListenerContainer container, boolean batchListener) {
                if (isFatalException(exception)) {
                    log.error("Fatal Kafka consumer exception occurred. Shutting down the application.", exception);
                    healthIndicator.markUnhealthy("Fatal Kafka exception: " + exception.getClass().getSimpleName() + " - " + exception.getMessage());
                } else {
                    // Delegate to default behavior for other exceptions
                    super.handleOtherException(exception, consumer, container, batchListener);
                }
            }
        };
    }

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
                                                                                                 HorizonTracer horizonTracer,
                                                                                                 CommonErrorHandler kafkaErrorHandler) {
        var containerProperties = new ContainerProperties(galaxyConfig.getConsumingTopic());
        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL);
        containerProperties.setMessageListener(new PublishedMessageListener(publishedMessageTaskFactory, horizonTracer, galaxyConfig));
        ConcurrentMessageListenerContainer<String, String> listenerContainer = new ConcurrentMessageListenerContainer<>(consumerFactory, containerProperties);
        listenerContainer.setAutoStartup(false);
        listenerContainer.setConcurrency(props.getPartitionCount());
        // bean name is the prefix of kafka consumer thread name
        listenerContainer.setBeanName("kafka-message-listener");
        listenerContainer.setCommonErrorHandler(kafkaErrorHandler);
        return listenerContainer;
    }

}
