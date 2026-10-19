// Copyright 2026 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.telekom.eni.pandora.horizon.cache.service.DeDuplicationService;
import de.telekom.eni.pandora.horizon.kafka.config.KafkaProperties;
import de.telekom.eni.pandora.horizon.kafka.event.EventWriter;
import de.telekom.eni.pandora.horizon.metrics.HorizonMetricsHelper;
import de.telekom.eni.pandora.horizon.tracing.HorizonTracer;
import de.telekom.horizon.galaxy.cache.PayloadSizeHistogramCache;
import de.telekom.horizon.galaxy.cache.SubscriberCache;
import de.telekom.horizon.galaxy.config.GalaxyConfig;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class PublishedMessageTaskTest {
    private HorizonTracer tracer;
    private EventWriter eventWriter;
    private HorizonMetricsHelper metricsHelper;
    private SubscriberCache subscriberCache;
    private DeDuplicationService deDuplicationService;
    private KafkaProperties kafkaProperties;
    private PayloadSizeHistogramCache incomingPayloadSizeCache;
    private PayloadSizeHistogramCache outgoingPayloadSizeHistogramCache;
    private GalaxyConfig galaxyConfig;
    private MeterRegistry meterRegistry;
    private Executor executor;
    private PublishedMessageTaskFactory publishedMessageTaskFactory;

    @BeforeEach
    void setUp() {
        tracer = mock(HorizonTracer.class);
        eventWriter = mock(EventWriter.class);
        metricsHelper = mock(HorizonMetricsHelper.class);
        subscriberCache = mock(SubscriberCache.class);
        deDuplicationService = mock(DeDuplicationService.class);
        kafkaProperties = mock(KafkaProperties.class);
        incomingPayloadSizeCache = mock(PayloadSizeHistogramCache.class);
        outgoingPayloadSizeHistogramCache = mock(PayloadSizeHistogramCache.class);
        galaxyConfig = mock(GalaxyConfig.class);
        meterRegistry = mock(MeterRegistry.class);
        executor = mock(Executor.class);

        publishedMessageTaskFactory = mock(PublishedMessageTaskFactory.class);
        when(publishedMessageTaskFactory.getTracer()).thenReturn(tracer);
        when(publishedMessageTaskFactory.getObjectMapper()).thenReturn(new ObjectMapper());
        when(publishedMessageTaskFactory.getIncomingPayloadSizeCache()).thenReturn(incomingPayloadSizeCache);
        when(publishedMessageTaskFactory.getSubscriptionCache()).thenReturn(subscriberCache);
        when(publishedMessageTaskFactory.getDeDuplicationService()).thenReturn(deDuplicationService);
        when(publishedMessageTaskFactory.getEventWriter()).thenReturn(eventWriter);
        when(publishedMessageTaskFactory.getMetricsHelper()).thenReturn(metricsHelper);
        when(publishedMessageTaskFactory.getKafkaProperties()).thenReturn(kafkaProperties);
        when(publishedMessageTaskFactory.getOutgoingPayloadSizeHistogramCache()).thenReturn(outgoingPayloadSizeHistogramCache);
        when(publishedMessageTaskFactory.getGalaxyConfig()).thenReturn(galaxyConfig);
        when(publishedMessageTaskFactory.getMeterRegistry()).thenReturn(meterRegistry);
        when(publishedMessageTaskFactory.getExecutor()).thenReturn(executor);
    }

    @Test
    void callShouldReturnSuccessfulResultAndSkipProcessingWhenPayloadIsNotParseable() {
        var consumerRecord = new ConsumerRecord<>("published", 0, 0L, "key", "{ \"foo\":");
        var result = new PublishedMessageTask(consumerRecord, publishedMessageTaskFactory).call();

        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());
        assertFalse(result.isCancelled());

        verifyNoInteractions(tracer,
                eventWriter,
                metricsHelper,
                subscriberCache,
                deDuplicationService,
                kafkaProperties,
                incomingPayloadSizeCache,
                outgoingPayloadSizeHistogramCache,
                galaxyConfig,
                meterRegistry,
                executor);
    }

    @Test
    void callShouldReturnSuccessfulResultAndSkipProcessingWhenPayloadIsNull() {
        var consumerRecord = new ConsumerRecord<String, String>("published", 0, 0L, "key", null);

        var task = new PublishedMessageTask(consumerRecord, publishedMessageTaskFactory);

        assertThrows(IllegalArgumentException.class, task::call);
        verifyNoInteractions(tracer,
                eventWriter,
                metricsHelper,
                subscriberCache,
                deDuplicationService,
                kafkaProperties,
                incomingPayloadSizeCache,
                outgoingPayloadSizeHistogramCache,
                galaxyConfig,
                meterRegistry,
                executor);
    }

    @Test
    void callShouldReturnSuccessfulResultAndSkipProcessingWhenPayloadIsJsonNull() {
        var consumerRecord = new ConsumerRecord<>("published", 0, 0L, "key", "null");

        var task = new PublishedMessageTask(consumerRecord, publishedMessageTaskFactory);

        assertThrows(NullPointerException.class, task::call);
        verifyNoInteractions(tracer,
                eventWriter,
                metricsHelper,
                subscriberCache,
                deDuplicationService,
                kafkaProperties,
                incomingPayloadSizeCache,
                outgoingPayloadSizeHistogramCache,
                galaxyConfig,
                meterRegistry,
                executor);
    }

    @Test
    void callShouldSetLoggingContextAndRecordIncomingPayloadSize() {
        var loggingContext = new HashMap<String, String>();
        doAnswer(invocation -> {
            loggingContext.put("UUID", MDC.get("UUID"));
            loggingContext.put("EventId", MDC.get("EventId"));
            return null;
        }).when(incomingPayloadSizeCache).recordMessage(any());

        var consumerRecord = new ConsumerRecord<>("published", 0, 0L, "key", "{\"uuid\": \"uuid-1\", \"event\": {\"id\": \"event-1\"}}");
        new PublishedMessageTask(consumerRecord, publishedMessageTaskFactory).call();

        verify(incomingPayloadSizeCache).recordMessage(argThat(message -> "uuid-1".equals(message.getUuid())));
        assertEquals(Map.of("UUID", "uuid-1", "EventId", "event-1"), loggingContext);
    }

    @Test
    void callShouldThrowAndClearLoggingContextWhenEventIsMissing() {
        var consumerRecord = new ConsumerRecord<>("published", 0, 0L, "key", "{\"uuid\": \"uuid-1\"}");
        var task = new PublishedMessageTask(consumerRecord, publishedMessageTaskFactory);

        assertThrows(NullPointerException.class, task::call);
        assertNull(MDC.get("UUID"));
        verifyNoInteractions(tracer,
                eventWriter,
                metricsHelper,
                subscriberCache,
                deDuplicationService,
                kafkaProperties,
                incomingPayloadSizeCache,
                outgoingPayloadSizeHistogramCache,
                galaxyConfig,
                meterRegistry,
                executor);
    }

    @Test
    void call() {
    }
}