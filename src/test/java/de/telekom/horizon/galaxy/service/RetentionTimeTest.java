package de.telekom.horizon.galaxy.service;


import com.fasterxml.jackson.core.JsonProcessingException;
import de.telekom.eni.pandora.horizon.model.event.Event;
import de.telekom.eni.pandora.horizon.model.event.PublishedEventMessage;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.eni.pandora.horizon.model.event.SubscriptionEventMessage;
import de.telekom.eni.pandora.horizon.model.meta.EventRetentionTime;
import de.telekom.horizon.galaxy.utils.AbstractIntegrationTest;
import de.telekom.horizon.galaxy.utils.HorizonTestHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetentionTimeTest extends AbstractIntegrationTest {

    private final static String TRACING_HEADER_NAME = "x-b3-traceid";

    @Test
    void testMultiSubscription() throws InterruptedException, JsonProcessingException {
        // Foreach EventRetentionTime we create one subscriber (7d, 5d, 3d, 1d, 1h)
        Map<String, String> subscribersToRetentionTimes = Arrays.stream(EventRetentionTime.values())
                .collect(
                        Collectors.toMap(
                                ert -> String.format("subscriber_%s", ert.toRoverConfigString()),
                                EventRetentionTime::toRoverConfigString,
                                (roverConfigStr1, roverConfigStr2) -> roverConfigStr1

                        )
                );
        var retentionTimeTopics = Arrays.stream(EventRetentionTime.values()).map(EventRetentionTime::getTopic).collect(Collectors.toSet());
        subscribersToRetentionTimes.forEach(
                (subscriber, retentionTime) -> addTestSubscription(HorizonTestHelper.createDefaultSubscriptionResourceWithSubscriberId(TEST_ENVIRONMENT, getEventType(), retentionTime, subscriber)));

        final String traceId = UUID.randomUUID().toString();
        final String eventId = UUID.randomUUID().toString();
        final String messageUuid = UUID.randomUUID().toString();

        final String testEvent = """
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
        while ((received = pollForRecord(5, TimeUnit.SECONDS)) != null) {
            receivedList.add(received);
        }
        assertEquals(subscribersToRetentionTimes.size(), receivedList.size());

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
            final String subscriberId = (String) multiplexedEvent.getAdditionalFields().get("subscriber-id");

            final String expectedTopicStr = subscribersToRetentionTimes.get(subscriberId);
            final var expectedRetentionTime = EventRetentionTime.fromString(expectedTopicStr);
            final var actualTopic = stringStringConsumerRecord.topic();
            assertEquals(expectedRetentionTime.getTopic(), actualTopic);

            subscribersToRetentionTimes.remove(subscriberId);
            retentionTimeTopics.remove(actualTopic);

        });
        assertTrue(subscribersToRetentionTimes.isEmpty());
        assertTrue(retentionTimeTopics.isEmpty());
    }


}
