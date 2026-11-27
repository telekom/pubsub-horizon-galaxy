// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.telekom.eni.pandora.horizon.cache.service.DeDuplicationService;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.eni.pandora.horizon.model.event.Event;
import de.telekom.eni.pandora.horizon.model.event.PublishedEventMessage;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.eni.pandora.horizon.model.event.SubscriptionEventMessage;
import de.telekom.horizon.galaxy.model.EvaluationResultStatus;
import de.telekom.horizon.galaxy.utils.AbstractIntegrationTest;
import de.telekom.horizon.galaxy.utils.HazelcastTestInstance;
import de.telekom.horizon.galaxy.utils.HorizonTestHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(HazelcastTestInstance.class)
class SimpleGalaxyIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private DeDuplicationService deDuplicationService;

    @Autowired
    private ObjectMapper objectMapper;

    private final static String TEST_ENVIRONMENT = "playground";
    private final static String TRACING_HEADER_NAME = "x-b3-traceid";

    @Test
    void testDefaultBehaviour() throws InterruptedException, JsonProcessingException {
        //given
        SubscriptionResource subscription = HorizonTestHelper.createDefaultSubscriptionResource(TEST_ENVIRONMENT, getEventType());
        when(subscriptionCacheMock.getSubscriptionsForEnvironmentAndEventType(TEST_ENVIRONMENT, getEventType())).thenReturn(List.of(subscription));

        String traceId = UUID.randomUUID().toString();
        String eventId = UUID.randomUUID().toString();
        String messageUuid = UUID.randomUUID().toString();
        String testEvent = """
                {
                    "mydummydata": "foobar"
                }
                """;
        PublishedEventMessage message = new PublishedEventMessage();
        message.setUuid(messageUuid);
        message.setEnvironment(TEST_ENVIRONMENT);
        message.setHttpHeaders(
                Map.of("X-Pandora-Type", List.of("Test"), TRACING_HEADER_NAME, List.of(traceId))
        );
        Event event = new Event();
        event.setType(getEventType());
        event.setId(eventId);
        event.setData(testEvent);
        message.setEvent(event);
        message.setAdditionalFields(Map.of(TRACING_HEADER_NAME, traceId));

        simulateNewPublishedEvent(message);

        //then
        ConsumerRecord<String, String> received = pollForRecord(3, TimeUnit.SECONDS);
        assertNotNull(received);

        SubscriptionEventMessage multiplexedEvent = objectMapper.readValue(received.value(), SubscriptionEventMessage.class);
        assertEquals(Status.PROCESSED, multiplexedEvent.getStatus());
        assertEquals(messageUuid, multiplexedEvent.getMultiplexedFrom());
        assertEquals(eventId, multiplexedEvent.getEvent().getId());
        assertEquals(getEventType(), multiplexedEvent.getEvent().getType());
        assertEquals(testEvent, multiplexedEvent.getEvent().getData());
        assertEquals(traceId, multiplexedEvent.getHttpHeaders().get(TRACING_HEADER_NAME).getFirst());
        assertEquals(traceId, multiplexedEvent.getAdditionalFields().get(TRACING_HEADER_NAME));

        EvaluationResultStatus scopeEvaluation = EvaluationResultStatus.valueOf((String) multiplexedEvent.getAdditionalFields().get("selectionFilterResult"));
        assertEquals(EvaluationResultStatus.NO_FILTER, scopeEvaluation);

        // atm no other possibility visible to check called track method as to check for isduplicate after simple integration test here
        assertTrue(deDuplicationService.isDuplicate(message, subscription.getSpec().getSubscription().getSubscriptionId()));
    }

    @Test
    void testConsumerRecoversWhenMaxPollIntervalExceeded() throws InterruptedException, JsonProcessingException {
        // given
        SubscriptionResource subscription = HorizonTestHelper.createDefaultSubscriptionResource(TEST_ENVIRONMENT, getEventType());
        AtomicBoolean firstCall = new AtomicBoolean(true);

        when(subscriptionCacheMock.getSubscriptionsForEnvironmentAndEventType(TEST_ENVIRONMENT, getEventType()))
                .thenAnswer(invocation -> {
                    if (firstCall.compareAndSet(true, false)) {
                        // Exceed max.poll.interval.ms so the consumer is removed from the consumer group
                        Thread.sleep(70_000L);
                    }
                    return List.of(subscription);
                });

        PublishedEventMessage firstMessage = createPublishedEventMessage();
        PublishedEventMessage secondMessage = createPublishedEventMessage();

        // when: first message triggers slow processing that exceeds max.poll.interval.ms
        simulateNewPublishedEvent(firstMessage);

        ConsumerRecord<String, String> firstReceived = pollForRecord(75, TimeUnit.SECONDS);
        assertNotNull(firstReceived);

        SubscriptionEventMessage firstMultiplexed = objectMapper.readValue(firstReceived.value(), SubscriptionEventMessage.class);
        assertEquals(Status.PROCESSED, firstMultiplexed.getStatus());
        assertEquals(firstMessage.getUuid(), firstMultiplexed.getMultiplexedFrom());

        // when: second message is published after consumer was removed from group and polls again
        simulateNewPublishedEvent(secondMessage);

        // then: consumer rejoins the group on poll and processes the new message
        ConsumerRecord<String, String> secondReceived = pollForRecord(15, TimeUnit.SECONDS);
        assertNotNull(secondReceived);

        SubscriptionEventMessage secondMultiplexed = objectMapper.readValue(secondReceived.value(), SubscriptionEventMessage.class);
        if (firstMessage.getUuid().equals(secondMultiplexed.getMultiplexedFrom())) {
            // If uncommitted first message was redelivered after rejoin, poll the next record for the second message
            secondReceived = pollForRecord(15, TimeUnit.SECONDS);
            assertNotNull(secondReceived);
            secondMultiplexed = objectMapper.readValue(secondReceived.value(), SubscriptionEventMessage.class);
        }

        assertEquals(Status.PROCESSED, secondMultiplexed.getStatus());
        assertEquals(secondMessage.getUuid(), secondMultiplexed.getMultiplexedFrom());
        assertEquals(secondMessage.getEvent().getId(), secondMultiplexed.getEvent().getId());
    }

    private PublishedEventMessage createPublishedEventMessage() {
        String traceId = UUID.randomUUID().toString();
        String eventId = UUID.randomUUID().toString();
        String messageUuid = UUID.randomUUID().toString();
        String testEvent = """
                {
                    "mydummydata": "foobar"
                }
                """;
        PublishedEventMessage message = new PublishedEventMessage();
        message.setUuid(messageUuid);
        message.setEnvironment(TEST_ENVIRONMENT);
        message.setHttpHeaders(
                Map.of("X-Pandora-Type", List.of("Test"), TRACING_HEADER_NAME, List.of(traceId))
        );
        Event event = new Event();
        event.setType(getEventType());
        event.setId(eventId);
        event.setData(testEvent);
        message.setEvent(event);
        message.setAdditionalFields(Map.of(TRACING_HEADER_NAME, traceId));
        return message;
    }
}
