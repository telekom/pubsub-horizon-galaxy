// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.config.kafka;

import de.telekom.eni.pandora.horizon.kafka.config.KafkaProperties;
import de.telekom.eni.pandora.horizon.tracing.HorizonTracer;
import de.telekom.horizon.galaxy.config.GalaxyConfig;
import de.telekom.horizon.galaxy.kafka.KafkaConsumerHealthIndicator;
import de.telekom.horizon.galaxy.kafka.PublishedMessageListener;
import de.telekom.horizon.galaxy.kafka.PublishedMessageTaskFactory;
import de.telekom.horizon.galaxy.service.BackpressureHandler;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.InterruptException;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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

    @Value("${horizon.kafka.fatalExceptionHandlingEnabled:true}")
    private boolean fatalExceptionHandlingEnabled;

    @Bean
    public PublishedMessageListener publishedMessageListener(PublishedMessageTaskFactory publishedMessageTaskFactory,
                                                             HorizonTracer horizonTracer,
                                                             GalaxyConfig galaxyConfig,
                                                             MeterRegistry meterRegistry) {
        return new PublishedMessageListener(publishedMessageTaskFactory, horizonTracer, galaxyConfig, meterRegistry);
    }

    /**
     * Creates a custom error handler that marks the application as unhealthy when a fatal exception
     * occurs during Kafka poll(). This allows Kubernetes to detect the unhealthy state via the
     * health endpoint and restart the pod.
     *
     * <p>This behavior can be disabled via configuration:</p>
     * <pre>horizon.kafka.fatalExceptionHandlingEnabled: false</pre>
     *
     * <p>Fatal exceptions include:</p>
     * <ul>
     *   <li>{@link InterruptException} / {@link InterruptedException} - Thread interrupted during poll</li>
     *   <li>{@link AuthenticationException} - SASL/SSL authentication failed</li>
     *   <li>{@link AuthorizationException} - No permission to access topic/group</li>
     *   <li>{@link FencedInstanceIdException} - Static member fenced by another instance</li>
     * </ul>
     *
     * <p>Record-level exceptions use the default behavior (retry + log + skip).</p>
     *
     * @param healthIndicator Health indicator to mark as unhealthy on fatal exceptions
     * @return CommonErrorHandler that handles fatal exceptions during poll() by marking health as DOWN
     */
    @Bean
    public CommonErrorHandler kafkaErrorHandler(KafkaConsumerHealthIndicator healthIndicator) {
        return new DefaultErrorHandler() {
            @Override
            public void handleOtherException(@NotNull Exception exception, @NotNull org.apache.kafka.clients.consumer.Consumer<?, ?> consumer,
                                             @NotNull MessageListenerContainer container, boolean batchListener) {
                if (fatalExceptionHandlingEnabled && isFatalException(exception)) {
                    log.error("Fatal Kafka consumer exception occurred. Marking health as DOWN.", exception);
                    healthIndicator.markUnhealthy("Fatal Kafka consumer exception occurred");
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
    public ConcurrentMessageListenerContainer<String, String> concurrentMessageListenerContainer(PublishedMessageListener listener,
                                                                                                 KafkaProperties props,
                                                                                                 ConsumerFactory<String, String> consumerFactory,
                                                                                                 GalaxyConfig galaxyConfig,
                                                                                                 BackpressureHandler backpressureHandler,
                                                                                                 PublishedMessageTaskFactory publishedMessageTaskFactory,
                                                                                                 CommonErrorHandler kafkaErrorHandler) {
        // Register backpressure handler as rejection handler on both executors
        listener.getTaskExecutor().getThreadPoolExecutor().setRejectedExecutionHandler(backpressureHandler);
        publishedMessageTaskFactory.getSubscriptionTaskExecutor().getThreadPoolExecutor().setRejectedExecutionHandler(backpressureHandler);

        // Set executor references on handler for resume monitoring
        backpressureHandler.setBatchExecutor(listener.getTaskExecutor());
        backpressureHandler.setSubscriptionExecutor(publishedMessageTaskFactory.getSubscriptionTaskExecutor());

        var containerProperties = new ContainerProperties(galaxyConfig.getConsumingTopic());
        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL);
        containerProperties.setMessageListener(listener);
        ConcurrentMessageListenerContainer<String, String> listenerContainer = new ConcurrentMessageListenerContainer<>(consumerFactory, containerProperties);
        listenerContainer.setAutoStartup(false);
        listenerContainer.setConcurrency(props.getPartitionCount());
        // bean name is the prefix of kafka consumer thread name
        listenerContainer.setBeanName("kafka-message-listener");
        listenerContainer.setCommonErrorHandler(kafkaErrorHandler);

        backpressureHandler.setListenerContainer(listenerContainer);

        return listenerContainer;
    }

    /**
     * Checks if the given exception or any of its causes is a fatal Kafka exception.
     *
     * @param exception The exception to check
     * @return true if the exception or any cause is fatal, false otherwise
     */
    private static boolean isFatalException(Throwable exception) {
        if (exception == null) {
            return false;
        }
        if (exception instanceof InterruptedException ||
            exception instanceof InterruptException ||
            exception instanceof AuthenticationException ||
            exception instanceof AuthorizationException ||
            exception instanceof FencedInstanceIdException) {
            return true;
        }
        return isFatalException(exception.getCause());
    }
}
