// Copyright 2026 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.telekom.eni.pandora.horizon.cache.service.DeDuplicationService;
import de.telekom.eni.pandora.horizon.kafka.event.EventWriter;
import de.telekom.eni.pandora.horizon.tracing.HorizonTracer;
import de.telekom.horizon.galaxy.cache.PayloadSizeHistogramCache;
import de.telekom.horizon.galaxy.cache.SubscriberCache;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import java.util.HashMap;
import java.util.Map;

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
    private PayloadSizeHistogramCache incomingPayloadSizeCache;
    private SubscriberCache subscriberCache;
    private DeDuplicationService deDuplicationService;
    private EventWriter eventWriter;
    private PublishedMessageTaskFactory publishedMessageTaskFactory;

    @BeforeEach
    void setUp() {
        tracer = mock(HorizonTracer.class);
        incomingPayloadSizeCache = mock(PayloadSizeHistogramCache.class);
        subscriberCache = mock(SubscriberCache.class);
        deDuplicationService = mock(DeDuplicationService.class);
        eventWriter = mock(EventWriter.class);
        publishedMessageTaskFactory = mock(PublishedMessageTaskFactory.class);

        when(publishedMessageTaskFactory.getTracer()).thenReturn(tracer);
        when(publishedMessageTaskFactory.getObjectMapper()).thenReturn(new ObjectMapper());
        when(publishedMessageTaskFactory.getIncomingPayloadSizeCache()).thenReturn(incomingPayloadSizeCache);
        when(publishedMessageTaskFactory.getSubscriptionCache()).thenReturn(subscriberCache);
        when(publishedMessageTaskFactory.getDeDuplicationService()).thenReturn(deDuplicationService);
        when(publishedMessageTaskFactory.getEventWriter()).thenReturn(eventWriter);
    }

    @Test
    void callShouldReturnSuccessfulResultAndSkipProcessingWhenPayloadIsNotParseable() {
        var consumerRecord = new ConsumerRecord<>("published", 0, 0L, "key", "{ \"foo\":");
        var result = new PublishedMessageTask(consumerRecord, publishedMessageTaskFactory).call();

        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());
        assertFalse(result.isCancelled());
        verifyNoInteractions(incomingPayloadSizeCache, subscriberCache, deDuplicationService, eventWriter);
    }

    @Test
    void callShouldReturnSuccessfulResultAndSkipProcessingWhenPayloadIsNull() {
        var consumerRecord = new ConsumerRecord<String, String>("published", 0, 0L, "key", null);

        var task = new PublishedMessageTask(consumerRecord, publishedMessageTaskFactory);

        assertThrows(IllegalArgumentException.class, task::call);
        verifyNoInteractions(incomingPayloadSizeCache, subscriberCache, deDuplicationService, eventWriter);
    }

    @Test
    void callShouldReturnSuccessfulResultAndSkipProcessingWhenPayloadIsJsonNull() {
        var consumerRecord = new ConsumerRecord<>("published", 0, 0L, "key", "null");

        var task = new PublishedMessageTask(consumerRecord, publishedMessageTaskFactory);

        assertThrows(NullPointerException.class, task::call);
        verifyNoInteractions(incomingPayloadSizeCache, subscriberCache, deDuplicationService, eventWriter);
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
        verifyNoInteractions(incomingPayloadSizeCache, subscriberCache, deDuplicationService, eventWriter);
    }

}