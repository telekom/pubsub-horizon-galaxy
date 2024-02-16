// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import de.telekom.eni.pandora.horizon.model.event.Event;
import de.telekom.eni.pandora.horizon.model.event.PublishedEventMessage;
import de.telekom.horizon.galaxy.utils.AbstractIntegrationTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNull;


class EmptyRecipientsTest extends AbstractIntegrationTest {

    private final static String TEST_ENVIRONMENT = "playground";
    private final static String TRACING_HEADER_NAME = "x-b3-traceid";

    @Test
    void shouldNotSendResultMessageWithoutSubscribers() throws InterruptedException, JsonProcessingException {
        //given
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
        ConsumerRecord<String, String> received = pollForRecord(3, TimeUnit.SECONDS);
        assertNull(received);

    }

}
