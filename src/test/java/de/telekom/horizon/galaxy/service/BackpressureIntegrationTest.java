// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.telekom.eni.pandora.horizon.model.event.SubscriptionEventMessage;
import de.telekom.horizon.galaxy.utils.AbstractIntegrationTest;
import de.telekom.horizon.galaxy.utils.HazelcastTestInstance;
import de.telekom.horizon.galaxy.utils.HorizonTestHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(HazelcastTestInstance.class)
class BackpressureIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private BackpressureHandler backpressureHandler;

    @Autowired
    private ObjectMapper objectMapper;

    private static final String TEST_ENVIRONMENT = "playground";

    @BeforeEach
    void resetBackpressure() {
        if (backpressureHandler.isPaused()) {
            backpressureHandler.resume();
        }
    }

    @Test
    void backpressurePauseShouldStopConsumptionAndResumeAfterRecovery() throws Exception {
        // given: a subscription for the event type
        var sub = HorizonTestHelper.createDefaultSubscriptionResource(TEST_ENVIRONMENT, getEventType());
        when(subscriptionCacheMock.getSubscriptionsForEnvironmentAndEventType(TEST_ENVIRONMENT, getEventType()))
                .thenReturn(List.of(sub));

        // when: simulate backpressure (this is what RejectedExecutionHandler.rejectedExecution does)
        backpressureHandler.pause();
        assertTrue(backpressureHandler.isPaused());

        // publish an event while paused
        var message = getPublishedEventMessage("{\"data\": \"backpressure-test\"}");
        simulateNewPublishedEvent(message);

        // then: no message should arrive while paused
        ConsumerRecord<String, String> blocked = pollForRecord(2, TimeUnit.SECONDS);
        assertNull(blocked, "No message should be received while consumer is paused");

        // when: resume (this is what checkAndResume does when pools recover)
        backpressureHandler.resume();
        assertFalse(backpressureHandler.isPaused());

        // then: message should be processed after resume
        ConsumerRecord<String, String> received = pollForRecord(10, TimeUnit.SECONDS);
        assertNotNull(received, "Event should be received after backpressure recovery");

        SubscriptionEventMessage multiplexedEvent = objectMapper.readValue(received.value(), SubscriptionEventMessage.class);
        assertEquals(getEventType(), multiplexedEvent.getEvent().getType());
        assertEquals(message.getUuid(), multiplexedEvent.getMultiplexedFrom());
    }
}
