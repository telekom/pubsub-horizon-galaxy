// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.eni.pandora.horizon.model.event.Event;
import de.telekom.eni.pandora.horizon.model.event.PublishedEventMessage;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.eni.pandora.horizon.model.event.SubscriptionEventMessage;
import de.telekom.horizon.galaxy.utils.AbstractIntegrationTest;
import de.telekom.horizon.galaxy.utils.HazelcastTestInstance;
import de.telekom.horizon.galaxy.utils.HorizonTestHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(HazelcastTestInstance.class)
class MultisubscriptionTest extends AbstractIntegrationTest {

    @Autowired
    private ObjectMapper objectMapper;

    private final static String TEST_ENVIRONMENT = "playground";
    private final static String TRACING_HEADER_NAME = "x-b3-traceid";

    @Test
    void testMultiSubscription() throws InterruptedException, JsonProcessingException {
        //given
        Set<String> subscriberIds = new HashSet<>(Arrays.asList("#1", "#2", "#3", "#4", "#5", "#6"));
        var subscriptions =  new ArrayList<SubscriptionResource>();

        subscriberIds.forEach(
                id -> {
                    subscriptions.add(HorizonTestHelper.createDefaultSubscriptionResourceWithSubscriberId(TEST_ENVIRONMENT, getEventType(), id));
                });

        when(subscriptionCacheMock.getSubscriptionsForEnvironmentAndEventType(TEST_ENVIRONMENT, getEventType())).thenReturn(subscriptions);

        String traceId = UUID.randomUUID().toString();
        String eventId = UUID.randomUUID().toString();
        String messageUuid = UUID.randomUUID().toString();

        String testEvent = """
                {
                    "mydummydata": "foobar"
                }
                """;

        //when
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
        List<ConsumerRecord<String, String>> receivedList = new LinkedList<>();

        ConsumerRecord<String, String> received;
        while ((received = pollForRecord(3, TimeUnit.SECONDS)) != null) {
            receivedList.add(received);
        }
        assertEquals(subscriberIds.size(), receivedList.size());

        receivedList.forEach(stringStringConsumerRecord -> {
            SubscriptionEventMessage multiplexedEvent;
            try {
                multiplexedEvent = objectMapper.readValue(stringStringConsumerRecord.value(), SubscriptionEventMessage.class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            assertEquals(Status.PROCESSED, multiplexedEvent.getStatus());
            assertEquals(messageUuid, multiplexedEvent.getMultiplexedFrom());
            assertEquals(eventId, multiplexedEvent.getEvent().getId());
            assertEquals(getEventType(), multiplexedEvent.getEvent().getType());
            assertEquals(testEvent, multiplexedEvent.getEvent().getData());
            assertEquals(traceId, multiplexedEvent.getHttpHeaders().get(TRACING_HEADER_NAME).get(0));
            assertEquals(traceId, multiplexedEvent.getAdditionalFields().get(TRACING_HEADER_NAME));

            subscriberIds.remove(multiplexedEvent.getAdditionalFields().get("subscriber-id"));
        });
        assertTrue(subscriberIds.isEmpty());

    }

}
