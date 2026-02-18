// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.telekom.eni.pandora.horizon.cache.service.DeDuplicationService;
import de.telekom.eni.pandora.horizon.kafka.config.KafkaProperties;
import de.telekom.eni.pandora.horizon.kafka.event.EventWriter;
import de.telekom.eni.pandora.horizon.metrics.HorizonMetricsHelper;
import de.telekom.eni.pandora.horizon.model.event.PublishedEventMessage;
import de.telekom.eni.pandora.horizon.tracing.HorizonTracer;
import de.telekom.horizon.galaxy.cache.PayloadSizeHistogramCache;
import de.telekom.horizon.galaxy.cache.SubscriberCache;
import de.telekom.horizon.galaxy.config.GalaxyConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.Collections;

/**
 * Factory for creating tasks associated with a published message.
 * @see PublishedEventMessage
 * @see PublishedMessageTask
 */
@Component
@Getter
public class PublishedMessageTaskFactory {

    private final HorizonTracer tracer;
    private final EventWriter eventWriter;
    private final HorizonMetricsHelper metricsHelper;
    private final SubscriberCache subscriptionCache;
    private final DeDuplicationService deDuplicationService;
    private final KafkaProperties kafkaProperties;
    private final PayloadSizeHistogramCache incomingPayloadSizeCache;
    private final PayloadSizeHistogramCache outgoingPayloadSizeHistogramCache;
    private final GalaxyConfig galaxyConfig;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;
    private final ThreadPoolTaskExecutor subscriptionTaskExecutor;
    private final Counter subscriptionThreadPoolSaturatedCounter;

    @Autowired
    public PublishedMessageTaskFactory(HorizonTracer tracer, EventWriter eventWriter, HorizonMetricsHelper metricsHelper, SubscriberCache subscriptionCache, DeDuplicationService deDuplicationService, KafkaProperties kafkaProperties, @Qualifier("incomingPayloadSizeCache") PayloadSizeHistogramCache incomingPayloadSizeCache, @Qualifier("outgoingPayloadSizeCache") PayloadSizeHistogramCache outgoingPayloadSizeHistogramCache, GalaxyConfig galaxyConfig, ObjectMapper objectMapper, MeterRegistry meterRegistry) {
        this.tracer = tracer;
        this.eventWriter = eventWriter;
        this.metricsHelper = metricsHelper;
        this.subscriptionCache = subscriptionCache;
        this.deDuplicationService = deDuplicationService;
        this.kafkaProperties = kafkaProperties;
        this.incomingPayloadSizeCache = incomingPayloadSizeCache;
        this.outgoingPayloadSizeHistogramCache = outgoingPayloadSizeHistogramCache;
        this.galaxyConfig = galaxyConfig;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
        this.subscriptionTaskExecutor = initSubscriptionThreadPoolTaskExecutor();
        this.subscriptionThreadPoolSaturatedCounter = Counter.builder("pubsub.subscription.threadpool.saturated")
                .description("Number of times the subscription thread pool rejected tasks due to queue saturation")
                .register(meterRegistry);
    }

    private ThreadPoolTaskExecutor initSubscriptionThreadPoolTaskExecutor() {
        final ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setThreadGroupName("multiplex");
        threadPoolTaskExecutor.setThreadNamePrefix("multiplex-");
        threadPoolTaskExecutor.setCorePoolSize(galaxyConfig.getSubscriptionCoreThreadPoolSize());
        threadPoolTaskExecutor.setMaxPoolSize(galaxyConfig.getSubscriptionMaxThreadPoolSize());
        threadPoolTaskExecutor.setQueueCapacity(galaxyConfig.getSubscriptionQueueCapacity());
        threadPoolTaskExecutor.setPrestartAllCoreThreads(true);
        threadPoolTaskExecutor.afterPropertiesSet();
        
        // Register metrics for the thread pool
        ExecutorServiceMetrics.monitor(meterRegistry, threadPoolTaskExecutor.getThreadPoolExecutor(), 
            "subscriptionTaskExecutor", Collections.emptyList());
        
        return threadPoolTaskExecutor;
    }

    public PublishedMessageTask newTask(ConsumerRecord<String, String> consumerRecord) {
        return new PublishedMessageTask(consumerRecord, this);
    }

    public void incrementSubscriptionThreadPoolSaturatedCounter() {
        subscriptionThreadPoolSaturatedCounter.increment();
    }
}
