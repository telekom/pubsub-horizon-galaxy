// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import de.telekom.eni.pandora.horizon.model.event.PublishedEventMessage;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.horizon.galaxy.utils.AbstractIntegrationTest;
import de.telekom.horizon.galaxy.utils.HazelcastTestInstance;
import de.telekom.horizon.galaxy.utils.HorizonTestHelper;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(HazelcastTestInstance.class)
class ResponseFilterHandlingTest extends AbstractIntegrationTest {

    @Test
    void onlyFilteredFieldsShouldBeSent() throws JsonProcessingException {
        var subscription = HorizonTestHelper.getSubscriptionResourceWithResponseFilter(TEST_ENVIRONMENT, getEventType());
        when(subscriptionCacheMock.getSubscriptionsForEnvironmentAndEventType(TEST_ENVIRONMENT, getEventType())).thenReturn(List.of(subscription));

        @Language("JSON") String testEvent = """
                {
                    "foo":{
                      "bar": "I'm foo.bar",
                      "secret": "I'm a secret"
                    },
                    "bar": {
                      "foo": "I'm also a secret"
                    }
                }""";

        PublishedEventMessage inboundMessage = getPublishedEventMessage(testEvent);
        simulateNewPublishedEvent(inboundMessage);

        var outboundMessages = receiveOutboundEvents();

        @Language("JSON") String expectedOutboundData = """
                {
                  "foo": {
                    "bar": "I'm foo.bar"
                  }
                }
                """;
        assertNotEquals(0, outboundMessages.size());
        var outboundMessage = outboundMessages.get(0);
        assertEquals(1, outboundMessages.size());

        assertEquals(Status.PROCESSED, outboundMessage.getStatus());
        assertEquals(objectMapper.readTree(expectedOutboundData), objectMapper.readTree(objectMapper.valueToTree(outboundMessage.getEvent().getData()).toString()));
    }

    @Test
    void messageShouldBeSentIfAllFieldsAreFiltered() throws JsonProcessingException {

        var subscription = HorizonTestHelper.getSubscriptionResourceWithResponseFilter(TEST_ENVIRONMENT, getEventType());
        when(subscriptionCacheMock.getSubscriptionsForEnvironmentAndEventType(TEST_ENVIRONMENT, getEventType()))
                .thenReturn(List.of(subscription));

        @Language("JSON") String testEvent = """
                {
                  "foo": "bar"\s
                }""";

        PublishedEventMessage inboundMessage = getPublishedEventMessage(testEvent);
        simulateNewPublishedEvent(inboundMessage);

        var outboundMessages = receiveOutboundEvents();

        var outboundMessage = outboundMessages.get(0);
        assertEquals(1, outboundMessages.size());

        assertEquals(Status.PROCESSED, outboundMessage.getStatus());
        assertTrue(((LinkedHashMap<?, ?>) outboundMessage.getEvent().getData()).isEmpty());

    }

}
