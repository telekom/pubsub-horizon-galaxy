// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionTrigger;
import de.telekom.eni.pandora.horizon.model.event.DeliveryType;
import de.telekom.eni.pandora.horizon.model.event.PublishedEventMessage;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.horizon.galaxy.model.EvaluationResultStatus;
import de.telekom.horizon.galaxy.utils.AbstractIntegrationTest;
import de.telekom.horizon.galaxy.utils.HazelcastTestInstance;
import de.telekom.horizon.galaxy.utils.HorizonTestHelper;
import de.telekom.jsonfilter.operator.comparison.GreaterEqualOperator;
import de.telekom.jsonfilter.operator.logic.AndOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.LinkedHashMap;
import java.util.List;

import static de.telekom.horizon.galaxy.utils.HorizonTestHelper.createDefaultSubscriptionResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.when;

@ExtendWith(HazelcastTestInstance.class)
class SelectionFilterHandlingTest extends AbstractIntegrationTest {

    @Test
        //UNMATCHED consumer filter should result in DROPPED message
    void unmatchedConsumerFilterShouldResultInDroppedMessage() throws JsonProcessingException {
        var subscription = HorizonTestHelper.getSubscriptionResourceWithSelectionFilter(TEST_ENVIRONMENT, getEventType());
        when(subscriptionCacheMock.getSubscriptionsForEnvironmentAndEventType(TEST_ENVIRONMENT, getEventType())).thenReturn(List.of(subscription));

        String testEvent = """
                {
                    "foo":"notBar",
                    "number": 12
                }""";

        PublishedEventMessage inboundMessage = getPublishedEventMessage(testEvent);
        simulateNewPublishedEvent(inboundMessage);

        var outboundMessages = receiveOutboundEvents();

        assertEquals(1, outboundMessages.size());
        var outboundMessage = outboundMessages.getFirst();

        assertEquals(Status.DROPPED, outboundMessage.getStatus());
        assertEquals(DeliveryType.CALLBACK, outboundMessage.getDeliveryType());
        assertEquals(EvaluationResultStatus.CONSUMER_FILTER_ERROR.toString(), outboundMessage.getAdditionalFields().get("selectionFilterResult"));
        assertEquals("and", ((LinkedHashMap<?, ?>) outboundMessage.getAdditionalFields().get("selectionFilterTrace")).get("operatorName"));
        assertFalse((Boolean) ((LinkedHashMap<?, ?>) outboundMessage.getAdditionalFields().get("selectionFilterTrace")).get("match"));
    }

    @Test
        //Single value should be filterable
    void singleValueShouldBeFilterable() throws JsonProcessingException {
        var subscriptionResource = createDefaultSubscriptionResource(TEST_ENVIRONMENT, getEventType());
        var subscription = subscriptionResource.getSpec().getSubscription();

        var filterOperator = new AndOperator(List.of(
                new GreaterEqualOperator<>("$", 12)
        ));

        var subscriptionTrigger = new SubscriptionTrigger();
        subscriptionTrigger.setAdvancedSelectionFilter(filterOperator);
        subscription.setTrigger(subscriptionTrigger);

        subscriptionResource.getSpec().setSubscription(subscription);

        when(subscriptionCacheMock.getSubscriptionsForEnvironmentAndEventType(TEST_ENVIRONMENT, getEventType())).thenReturn(List.of(subscriptionResource));

        PublishedEventMessage inboundMessage = getPublishedEventMessage(12);
        simulateNewPublishedEvent(inboundMessage);

        var outboundMessages = receiveOutboundEvents();

        assertEquals(1, outboundMessages.size());
        var outboundMessage = outboundMessages.getFirst();

        assertEquals(Status.PROCESSED, outboundMessage.getStatus());
        assertEquals(EvaluationResultStatus.MATCH.toString(), outboundMessage.getAdditionalFields().get("selectionFilterResult"));
        assertEquals(12, outboundMessage.getEvent().getData());
    }

    @Test
        //MATCHED consumer filter should result in PROCESSED message
    void matchedConsumerFilterShouldResultInProcessedMessage() throws JsonProcessingException {

        var subscription = HorizonTestHelper.getSubscriptionResourceWithSelectionFilter(TEST_ENVIRONMENT, getEventType());
        when(subscriptionCacheMock.getSubscriptionsForEnvironmentAndEventType(TEST_ENVIRONMENT, getEventType())).thenReturn(List.of(subscription));

        String testEvent = """
                {
                    "foo":"bar",
                    "number": 12
                }""";

        PublishedEventMessage inboundMessage = getPublishedEventMessage(testEvent);
        simulateNewPublishedEvent(inboundMessage);

        var outboundMessages = receiveOutboundEvents();

        assertEquals(1, outboundMessages.size());
        var outboundMessage = outboundMessages.getFirst();

        assertEquals(Status.PROCESSED, outboundMessage.getStatus());
        assertEquals(EvaluationResultStatus.MATCH.toString(), outboundMessage.getAdditionalFields().get("selectionFilterResult"));
    }

    @Test
        //Empty payload should result in DROPPED message when filter IS set
    void emptyPayloadShouldResultInDroppedMessageWhenFilterIsSet() throws JsonProcessingException {

        var subscription = HorizonTestHelper.getSubscriptionResourceWithSelectionFilter(TEST_ENVIRONMENT, getEventType());
        when(subscriptionCacheMock.getSubscriptionsForEnvironmentAndEventType(TEST_ENVIRONMENT, getEventType())).thenReturn(List.of(subscription));

        PublishedEventMessage inboundMessage = getPublishedEventMessage("");
        simulateNewPublishedEvent(inboundMessage);

        var outboundMessageList = receiveOutboundEvents();

        assertEquals(1, outboundMessageList.size());
        var outboundMessage = outboundMessageList.getFirst();

        assertEquals(Status.DROPPED, outboundMessage.getStatus());
        assertEquals(EvaluationResultStatus.INVALID_PAYLOAD_ERROR.toString(), outboundMessage.getAdditionalFields().get("selectionFilterResult"));
        assertEquals("The payload in the incoming event was no valid JSON. Therefore, the filters could not be applied.", ((LinkedHashMap<?, ?>) outboundMessage.getAdditionalFields().get("selectionFilterTrace")).get("causeDescription"));
    }

    @Test
        // Empty payload should result in PROCESSED message when filter IS NOT set
    void emptyPayloadShouldResultInProcessedMessageWhenFilterIsNotSet() throws JsonProcessingException {

        var subscription = HorizonTestHelper.createDefaultSubscriptionResource(TEST_ENVIRONMENT, getEventType());
        when(subscriptionCacheMock.getSubscriptionsForEnvironmentAndEventType(TEST_ENVIRONMENT, getEventType())).thenReturn(List.of(subscription));

        PublishedEventMessage inboundMessage = getPublishedEventMessage("");
        simulateNewPublishedEvent(inboundMessage);

        var outboundMessageList = receiveOutboundEvents();

        assertEquals(1, outboundMessageList.size());
        var outboundMessage = outboundMessageList.getFirst();

        assertEquals(Status.PROCESSED, outboundMessage.getStatus());
        assertEquals(EvaluationResultStatus.NO_FILTER.toString(), outboundMessage.getAdditionalFields().get("selectionFilterResult"));
    }

    @Test
        //Invalid JSON payload should result in DROPPED message when filter IS set
    void invalidJsonPayloadShouldResultInDroppedMessageWhenFilterIsSet() throws JsonProcessingException {

        var subscription = HorizonTestHelper.getSubscriptionResourceWithSelectionFilter(TEST_ENVIRONMENT, getEventType());
        when(subscriptionCacheMock.getSubscriptionsForEnvironmentAndEventType(TEST_ENVIRONMENT, getEventType())).thenReturn(List.of(subscription));

        String testEvent = """
                {
                    imInvalid,
                    "number": 12
                }""";

        PublishedEventMessage inboundMessage = getPublishedEventMessage(testEvent);
        simulateNewPublishedEvent(inboundMessage);

        var outboundMessages = receiveOutboundEvents();

        assertEquals(1, outboundMessages.size());
        var outboundMessage = outboundMessages.getFirst();

        assertEquals(Status.DROPPED, outboundMessage.getStatus());
        assertEquals(EvaluationResultStatus.INVALID_PAYLOAD_ERROR.toString(), outboundMessage.getAdditionalFields().get("selectionFilterResult"));
        assertEquals("The payload in the incoming event was no valid JSON. Therefore, the filters could not be applied.", ((LinkedHashMap<?, ?>) outboundMessage.getAdditionalFields().get("selectionFilterTrace")).get("causeDescription"));
    }

    @Test
        //Invalid JSON payload should result in PROCESSED message when filter IS NOT set
    void invalidJsonPayloadShouldResultInProcessedMessageWhenFilterIsNotSet() throws JsonProcessingException {

        var subscription = HorizonTestHelper.createDefaultSubscriptionResource(TEST_ENVIRONMENT, getEventType());
        when(subscriptionCacheMock.getSubscriptionsForEnvironmentAndEventType(TEST_ENVIRONMENT, getEventType())).thenReturn(List.of(subscription));

        String testEvent = """
                {
                    imInvalid,
                    "number": 12
                }""";

        PublishedEventMessage inboundMessage = getPublishedEventMessage(testEvent);
        simulateNewPublishedEvent(inboundMessage);

        var outboundMessages = receiveOutboundEvents();

        assertEquals(1, outboundMessages.size());
        var outboundMessage = outboundMessages.getFirst();

        assertEquals(Status.PROCESSED, outboundMessage.getStatus());
        assertEquals(EvaluationResultStatus.NO_FILTER.toString(), outboundMessage.getAdditionalFields().get("selectionFilterResult"));
    }
}
