// Copyright 2026 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.kafka;

import brave.Span;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.telekom.eni.pandora.horizon.cache.service.DeDuplicationService;
import de.telekom.eni.pandora.horizon.kafka.config.KafkaProperties;
import de.telekom.eni.pandora.horizon.kafka.event.EventWriter;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionTrigger;
import de.telekom.eni.pandora.horizon.metrics.AdditionalFields;
import de.telekom.eni.pandora.horizon.metrics.HorizonMetricsHelper;
import de.telekom.eni.pandora.horizon.model.event.PublishedEventMessage;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.eni.pandora.horizon.model.event.StatusMessage;
import de.telekom.eni.pandora.horizon.model.event.SubscriptionEventMessage;
import de.telekom.eni.pandora.horizon.tracing.HorizonTracer;
import de.telekom.eni.pandora.horizon.tracing.ScopedDebugSpanWrapper;
import de.telekom.horizon.galaxy.cache.PayloadSizeHistogramCache;
import de.telekom.horizon.galaxy.cache.SubscriberCache;
import de.telekom.horizon.galaxy.config.GalaxyConfig;
import de.telekom.horizon.galaxy.model.EvaluationResultStatus;
import de.telekom.horizon.galaxy.utils.HorizonTestHelper;
import de.telekom.jsonfilter.operator.comparison.EqualsOperator;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.slf4j.MDC;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class PublishedMessageTaskTest {
    private static final String PUBLISHED_EVENT = """
            {"uuid": "uuid-1", "environment": "playground", "event": {"id": "event-1", "type": "my.event.type"}}
            """;
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
    private Span span;

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
        span = mock(Span.class);
        var scopedSpan = mock(ScopedDebugSpanWrapper.class);

        when(tracer.startSpan(anyString())).thenReturn(span);
        when(tracer.startScopedDebugSpan(anyString())).thenReturn(scopedSpan);

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

    @ParameterizedTest
    @NullAndEmptySource
    void callShouldReturnSuccessfulResultAndSkipMultiplexingWhenNoSubscriptionsFound(List<SubscriptionResource> subscriptions) {
        when(subscriberCache.getSubscriptionsForEnvironmentAndEventType("playground", "my.event.type")).thenReturn(subscriptions);

        var consumerRecord = new ConsumerRecord<>("published", 0, 0L, "key", PUBLISHED_EVENT);
        var result = new PublishedMessageTask(consumerRecord, publishedMessageTaskFactory).call();

        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());
        assertFalse(result.isCancelled());
        verify(subscriberCache).getSubscriptionsForEnvironmentAndEventType("playground", "my.event.type");
        verifyNoInteractions(deDuplicationService, eventWriter);
    }

    @Test
    void callShouldReturnSuccessfulResultAndSkipMultiplexingWhenAllSubscriptionsAreDuplicates() {
        var subscription = HorizonTestHelper.createDefaultSubscriptionResource("playground", "my.event.type");
        var subscriptionId = subscription.getSpec().getSubscription().getSubscriptionId();
        when(subscriberCache.getSubscriptionsForEnvironmentAndEventType("playground", "my.event.type")).thenReturn(List.of(subscription));
        when(deDuplicationService.isDuplicate(any(PublishedEventMessage.class), anyString())).thenReturn(true);

        var consumerRecord = new ConsumerRecord<>("published", 0, 0L, "key", PUBLISHED_EVENT);
        var result = new PublishedMessageTask(consumerRecord, publishedMessageTaskFactory).call();

        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());
        assertFalse(result.isCancelled());
        verify(deDuplicationService).isDuplicate(argThat((PublishedEventMessage message) -> "uuid-1".equals(message.getUuid())), eq(subscriptionId));
        verifyNoInteractions(eventWriter);
    }

    @Test
    void callShouldFilterOutDuplicateSubscriptionsAndMultiplexRemainingRecipients() throws Exception {
        var duplicateSub = HorizonTestHelper.createDefaultSubscriptionResource("playground", "my.event.type");
        var activeSub = HorizonTestHelper.createDefaultSubscriptionResource("playground", "my.event.type");
        var duplicateSubId = duplicateSub.getSpec().getSubscription().getSubscriptionId();
        var activeSubId = activeSub.getSpec().getSubscription().getSubscriptionId();

        when(subscriberCache.getSubscriptionsForEnvironmentAndEventType("playground", "my.event.type"))
                .thenReturn(List.of(duplicateSub, activeSub));
        when(deDuplicationService.isDuplicate(any(PublishedEventMessage.class), eq(duplicateSubId))).thenReturn(true);
        when(deDuplicationService.isDuplicate(any(PublishedEventMessage.class), eq(activeSubId))).thenReturn(false);
        when(eventWriter.send(anyString(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(executor).execute(any());
        when(metricsHelper.getRegistry()).thenReturn(new SimpleMeterRegistry());
        doAnswer(invocation -> invocation.getArgument(0))
                .when(tracer)
                .withSpanInScope(any(), any());

        var consumerRecord = new ConsumerRecord<>("published", 0, 0L, "key", PUBLISHED_EVENT);
        var result = new PublishedMessageTask(consumerRecord, publishedMessageTaskFactory).call();

        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());
        assertFalse(result.isCancelled());
        verify(eventWriter, times(1)).send(anyString(), argThat(msg ->
                msg instanceof SubscriptionEventMessage sem && activeSubId.equals(sem.getSubscriptionId())), any());
    }

    @Nested
    class EventDataParsingFilterEvaluation {

        @BeforeEach
        void setUp() throws JsonProcessingException {
            var galaxyConfig = new GalaxyConfig();
            galaxyConfig.setFeatureJsonPathFilteringEnabled(false);
            when(publishedMessageTaskFactory.getGalaxyConfig()).thenReturn(galaxyConfig);
            when(eventWriter.send(anyString(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));
            when(metricsHelper.getRegistry()).thenReturn(new SimpleMeterRegistry());
            doAnswer(invocation -> invocation.getArgument(0))
                    .when(tracer)
                    .withSpanInScope(any(), any());
            doAnswer(invocation -> {
                invocation.<Runnable>getArgument(0).run();
                return null;
            }).when(executor).execute(any());
        }

        // (eventData == null) -> INVALID_PAYLOAD_ERROR
        @Test
        void callShouldDropWithInvalidPayloadErrorWhenEventDataIsNull() throws Exception {
            var sub = HorizonTestHelper.getSubscriptionResourceWithSelectionFilter("playground", "my.event.type");
            when(subscriberCache.getSubscriptionsForEnvironmentAndEventType("playground", "my.event.type")).thenReturn(List.of(sub));

            var consumerRecord = new ConsumerRecord<>("published", 0, 0L, "key", PUBLISHED_EVENT);
            var result = new PublishedMessageTask(consumerRecord, publishedMessageTaskFactory).call();

            assertTrue(result.isDone());
            assertFalse(result.isCompletedExceptionally());
            assertFalse(result.isCancelled());
            verify(eventWriter).send(anyString(), argThat(msg ->
                    msg instanceof StatusMessage sm
                            && sm.getStatus() == Status.DROPPED
                            && EvaluationResultStatus.INVALID_PAYLOAD_ERROR.toString()
                            .equals(sm.getAdditionalFields().get(AdditionalFields.SELECTION_FILTER_RESULT.value))), any());
        }

        // non-JSON dataContentType -> INVALID_PAYLOAD_ERROR
        @Test
        void callShouldDropWithInvalidPayloadErrorWhenContentTypeIsNotJson() throws Exception {
            var sub = HorizonTestHelper.getSubscriptionResourceWithSelectionFilter("playground", "my.event.type");
            when(subscriberCache.getSubscriptionsForEnvironmentAndEventType("playground", "my.event.type")).thenReturn(List.of(sub));

            var record = """
            {"uuid":"uuid-1","environment":"playground","event":{"id":"event-1","type":"my.event.type","datacontenttype":"text/plain","data":"{\\"foo\\":\\"bar\\",\\"number\\":15}"}}""";

            var consumerRecord = new ConsumerRecord<>("published", 0, 0L, "key", record);
            var result = new PublishedMessageTask(consumerRecord, publishedMessageTaskFactory).call();

            assertTrue(result.isDone());
            assertFalse(result.isCompletedExceptionally());
            assertFalse(result.isCancelled());
            verify(eventWriter).send(anyString(), argThat(msg ->
                    msg instanceof StatusMessage sm
                            && sm.getStatus() == Status.DROPPED
                            && EvaluationResultStatus.INVALID_PAYLOAD_ERROR.toString()
                            .equals(sm.getAdditionalFields().get(AdditionalFields.SELECTION_FILTER_RESULT.value))), any());
        }

        // invalid JSON String -> JsonProcessingException -> INVALID_PAYLOAD_ERROR
        @Test
        void callShouldDropWithInvalidPayloadErrorWhenEventDataStringIsInvalidJson() throws Exception {
            var sub = HorizonTestHelper.getSubscriptionResourceWithSelectionFilter("playground", "my.event.type");
            when(subscriberCache.getSubscriptionsForEnvironmentAndEventType("playground", "my.event.type")).thenReturn(List.of(sub));

            var record = """
            {"uuid":"uuid-1","environment":"playground","event":{"id":"event-1","type":"my.event.type","datacontenttype":"application/json","data":"{invalid"}}""";

            var consumerRecord = new ConsumerRecord<>("published", 0, 0L, "key", record);
            var result = new PublishedMessageTask(consumerRecord, publishedMessageTaskFactory).call();

            assertTrue(result.isDone());
            assertFalse(result.isCompletedExceptionally());
            assertFalse(result.isCancelled());
            verify(eventWriter).send(anyString(), argThat(msg ->
                    msg instanceof StatusMessage sm
                            && sm.getStatus() == Status.DROPPED
                            && EvaluationResultStatus.INVALID_PAYLOAD_ERROR.toString()
                            .equals(sm.getAdditionalFields().get(AdditionalFields.SELECTION_FILTER_RESULT.value))), any());
        }

        // (empty String i.e. Jackson MissingNode) -> INVALID_PAYLOAD_ERROR
        @Test
        void callShouldDropWithInvalidPayloadErrorWhenEventDataParsesToMissingNode() throws Exception {
            var sub = HorizonTestHelper.getSubscriptionResourceWithSelectionFilter("playground", "my.event.type");
            when(subscriberCache.getSubscriptionsForEnvironmentAndEventType("playground", "my.event.type")).thenReturn(List.of(sub));

            var record = """
            {"uuid":"uuid-1","environment":"playground","event":{"id":"event-1","type":"my.event.type","datacontenttype":"application/json","data":""}}""";

            var consumerRecord = new ConsumerRecord<>("published", 0, 0L, "key", record);
            var result = new PublishedMessageTask(consumerRecord, publishedMessageTaskFactory).call();

            assertTrue(result.isDone());
            assertFalse(result.isCompletedExceptionally());
            assertFalse(result.isCancelled());
            verify(eventWriter).send(anyString(), argThat(msg ->
                    msg instanceof StatusMessage sm
                            && sm.getStatus() == Status.DROPPED
                            && EvaluationResultStatus.INVALID_PAYLOAD_ERROR.toString()
                            .equals(sm.getAdditionalFields().get(AdditionalFields.SELECTION_FILTER_RESULT.value))), any());
        }

        // NO_FILTER
        @Test
        void callShouldSendProcessedMessageWithNoFilterWhenSubscriptionHasNoFilters() throws Exception {
            var sub = HorizonTestHelper.createDefaultSubscriptionResource("playground", "my.event.type");
            when(subscriberCache.getSubscriptionsForEnvironmentAndEventType("playground", "my.event.type")).thenReturn(List.of(sub));

            var consumerRecord = new ConsumerRecord<>("published", 0, 0L, "key", PUBLISHED_EVENT);
            var result = new PublishedMessageTask(consumerRecord, publishedMessageTaskFactory).call();

            assertTrue(result.isDone());
            assertFalse(result.isCompletedExceptionally());
            assertFalse(result.isCancelled());
            verify(eventWriter).send(anyString(), argThat(msg ->
                    msg instanceof SubscriptionEventMessage sem
                            && sem.getStatus() == Status.PROCESSED
                            && EvaluationResultStatus.NO_FILTER.toString()
                            .equals(sem.getAdditionalFields().get(AdditionalFields.SELECTION_FILTER_RESULT.value))), any());
        }

        // SCOPE_FILTER_ERROR
        @Test
        void callShouldDropWithScopeFilterErrorWhenPublisherTriggerDoesNotMatch() throws Exception {
            var sub = HorizonTestHelper.createDefaultSubscriptionResource("playground", "my.event.type");
            var scopeTrigger = new SubscriptionTrigger();
            scopeTrigger.setAdvancedSelectionFilter(new EqualsOperator<>("$.foo", "expected"));
            sub.getSpec().getSubscription().setPublisherTrigger(scopeTrigger);
            when(subscriberCache.getSubscriptionsForEnvironmentAndEventType("playground", "my.event.type")).thenReturn(List.of(sub));

            var record = """
            {"uuid":"uuid-1","environment":"playground","event":{"id":"event-1","type":"my.event.type","data":{"foo":"actual"}}}""";

            var consumerRecord = new ConsumerRecord<>("published", 0, 0L, "key", record);
            var result = new PublishedMessageTask(consumerRecord, publishedMessageTaskFactory).call();

            assertTrue(result.isDone());
            assertFalse(result.isCompletedExceptionally());
            assertFalse(result.isCancelled());
            verify(eventWriter).send(anyString(), argThat(msg ->
                    msg instanceof StatusMessage sm
                            && sm.getStatus() == Status.DROPPED
                            && EvaluationResultStatus.SCOPE_FILTER_ERROR.toString()
                            .equals(sm.getAdditionalFields().get(AdditionalFields.SELECTION_FILTER_RESULT.value))), any());
        }

        // CONSUMER_FILTER_ERROR
        @Test
        void callShouldDropWithConsumerFilterErrorWhenConsumerTriggerDoesNotMatch() throws Exception {
            var sub = HorizonTestHelper.getSubscriptionResourceWithSelectionFilter("playground", "my.event.type");
            when(subscriberCache.getSubscriptionsForEnvironmentAndEventType("playground", "my.event.type")).thenReturn(List.of(sub));

            var record = """
            {"uuid":"uuid-1","environment":"playground","event":{"id":"event-1","type":"my.event.type","data":{"foo":"wrong","number":15}}}""";

            var consumerRecord = new ConsumerRecord<>("published", 0, 0L, "key", record);
            var result = new PublishedMessageTask(consumerRecord, publishedMessageTaskFactory).call();

            assertTrue(result.isDone());
            assertFalse(result.isCompletedExceptionally());
            assertFalse(result.isCancelled());
            verify(eventWriter).send(anyString(), argThat(msg ->
                    msg instanceof StatusMessage sm
                            && sm.getStatus() == Status.DROPPED
                            && EvaluationResultStatus.CONSUMER_FILTER_ERROR.toString()
                            .equals(sm.getAdditionalFields().get(AdditionalFields.SELECTION_FILTER_RESULT.value))), any());
        }

        // String eventData + null dataContentType -> MATCH
        @Test
        void callShouldSendFilteredProcessedMessageWhenStringEventDataMatchesAndContentTypeIsNull() throws Exception {
            var sub = HorizonTestHelper.getSubscriptionResourceWithSelectionFilter("playground", "my.event.type");
            sub.getSpec().getSubscription().getTrigger().setResponseFilter(List.of("foo"));
            when(subscriberCache.getSubscriptionsForEnvironmentAndEventType("playground", "my.event.type")).thenReturn(List.of(sub));

            var record = """
            {"uuid":"uuid-1","environment":"playground","event":{"id":"event-1","type":"my.event.type","data":"{\\"foo\\":\\"bar\\",\\"number\\":15}"}}""";

            var consumerRecord = new ConsumerRecord<>("published", 0, 0L, "key", record);
            var result = new PublishedMessageTask(consumerRecord, publishedMessageTaskFactory).call();

            assertTrue(result.isDone());
            assertFalse(result.isCompletedExceptionally());
            assertFalse(result.isCancelled());
            verify(eventWriter).send(anyString(), argThat(msg ->
                    msg instanceof SubscriptionEventMessage sem
                            && sem.getStatus() == Status.PROCESSED
                            && EvaluationResultStatus.MATCH.toString()
                            .equals(sem.getAdditionalFields().get(AdditionalFields.SELECTION_FILTER_RESULT.value))
                            && "{\"foo\":\"bar\"}".equals(sem.getEvent().getData().toString())), any());
        }

        // Object eventData + application/*+json dataContentType -> MATCH
        @Test
        void callShouldSendFilteredProcessedMessageWhenObjectEventDataMatchesAndContentTypeIsJson() throws Exception {
            var sub = HorizonTestHelper.getSubscriptionResourceWithSelectionFilter("playground", "my.event.type");
            sub.getSpec().getSubscription().getTrigger().setResponseFilter(List.of("foo"));
            when(subscriberCache.getSubscriptionsForEnvironmentAndEventType("playground", "my.event.type")).thenReturn(List.of(sub));

            var record = """
            {"uuid":"uuid-1","environment":"playground","event":{"id":"event-1","type":"my.event.type","datacontenttype":"application/cloudevents+json","data":{"foo":"bar","number":15}}}""";

            var consumerRecord = new ConsumerRecord<>("published", 0, 0L, "key", record);
            var result = new PublishedMessageTask(consumerRecord, publishedMessageTaskFactory).call();

            assertTrue(result.isDone());
            assertFalse(result.isCompletedExceptionally());
            assertFalse(result.isCancelled());
            verify(eventWriter).send(anyString(), argThat(msg ->
                    msg instanceof SubscriptionEventMessage sem
                            && sem.getStatus() == Status.PROCESSED
                            && EvaluationResultStatus.MATCH.toString()
                            .equals(sem.getAdditionalFields().get(AdditionalFields.SELECTION_FILTER_RESULT.value))
                            && "{\"foo\":\"bar\"}".equals(sem.getEvent().getData().toString())), any());
        }
    }
}