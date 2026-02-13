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
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

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

    private final Counter multiplexPoolsCreated;
    private final Counter multiplexTasksCompleted;
    private final AtomicInteger multiplexActiveThreads = new AtomicInteger(0);
    private final AtomicInteger multiplexPoolCount = new AtomicInteger(0);

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

        this.multiplexPoolsCreated = Counter.builder("galaxy.multiplex.pools.created")
                .description("Total number of multiplex thread pools created")
                .register(meterRegistry);
        this.multiplexTasksCompleted = Counter.builder("galaxy.multiplex.tasks.completed")
                .description("Total number of multiplex send tasks completed")
                .register(meterRegistry);

        Gauge.builder("galaxy.multiplex.pool.core.size", galaxyConfig, GalaxyConfig::getSubscriptionCoreThreadPoolSize)
                .description("Configured core pool size for multiplex thread pools")
                .register(meterRegistry);
        Gauge.builder("galaxy.multiplex.pool.max.size", galaxyConfig, GalaxyConfig::getSubscriptionMaxThreadPoolSize)
                .description("Configured max pool size for multiplex thread pools")
                .register(meterRegistry);

        meterRegistry.gauge("galaxy.multiplex.active.threads", multiplexActiveThreads);
        meterRegistry.gauge("galaxy.multiplex.pool.count", multiplexPoolCount);
    }

    public PublishedMessageTask newTask(ConsumerRecord<String, String> consumerRecord) {
        return new PublishedMessageTask(consumerRecord, this);
    }
}
