package de.telekom.horizon.galaxy.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import de.telekom.eni.jfilter.operator.comparison.GreaterEqualOperator;
import de.telekom.eni.jfilter.operator.logic.AndOperator;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionTrigger;
import de.telekom.eni.pandora.horizon.model.event.DeliveryType;
import de.telekom.eni.pandora.horizon.model.event.PublishedEventMessage;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.horizon.galaxy.model.EvaluationResultStatus;
import de.telekom.horizon.galaxy.utils.AbstractIntegrationTest;
import de.telekom.horizon.galaxy.utils.HorizonTestHelper;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;

import static de.telekom.horizon.galaxy.utils.HorizonTestHelper.createDefaultSubscriptionResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class SelectionFilterHandlingTest extends AbstractIntegrationTest {

    @Test
        //UNMATCHED consumer filter should result in DROPPED message
    void unmatchedConsumerFilterShouldResultInDroppedMessage() throws JsonProcessingException {
        addTestSubscription(HorizonTestHelper.getSubscriptionResourceWithSelectionFilter(TEST_ENVIRONMENT, getEventType()));
        String testEvent = """
                {
                    "foo":"notBar",
                    "number": 12
                }""";

        PublishedEventMessage inboundMessage = getPublishedEventMessage(testEvent);
        simulateNewPublishedEvent(inboundMessage);

        var outboundMessages = receiveOutboundEvents();

        assertEquals(1, outboundMessages.size());
        var outboundMessage = outboundMessages.get(0);

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

        addTestSubscription(subscriptionResource);

        PublishedEventMessage inboundMessage = getPublishedEventMessage(12);
        simulateNewPublishedEvent(inboundMessage);

        var outboundMessages = receiveOutboundEvents();

        assertEquals(1, outboundMessages.size());
        var outboundMessage = outboundMessages.get(0);

        assertEquals(Status.PROCESSED, outboundMessage.getStatus());
        assertEquals(EvaluationResultStatus.MATCH.toString(), outboundMessage.getAdditionalFields().get("selectionFilterResult"));
        assertEquals(12, outboundMessage.getEvent().getData());
    }

    @Test
        //MATCHED consumer filter should result in PROCESSED message
    void matchedConsumerFilterShouldResultInProcessedMessage() throws JsonProcessingException {
        addTestSubscription(HorizonTestHelper.getSubscriptionResourceWithSelectionFilter(TEST_ENVIRONMENT, getEventType()));
        String testEvent = """
                {
                    "foo":"bar",
                    "number": 12
                }""";

        PublishedEventMessage inboundMessage = getPublishedEventMessage(testEvent);
        simulateNewPublishedEvent(inboundMessage);

        var outboundMessages = receiveOutboundEvents();

        assertEquals(1, outboundMessages.size());
        var outboundMessage = outboundMessages.get(0);

        assertEquals(Status.PROCESSED, outboundMessage.getStatus());
        assertEquals(EvaluationResultStatus.MATCH.toString(), outboundMessage.getAdditionalFields().get("selectionFilterResult"));
    }

    @Test
        //Empty payload should result in DROPPED message when filter IS set
    void emptyPayloadShouldResultInDroppedMessageWhenFilterIsSet() throws JsonProcessingException {
        addTestSubscription(HorizonTestHelper.getSubscriptionResourceWithSelectionFilter(TEST_ENVIRONMENT, getEventType()));

        PublishedEventMessage inboundMessage = getPublishedEventMessage("");
        simulateNewPublishedEvent(inboundMessage);

        var outboundMessageList = receiveOutboundEvents();

        assertEquals(1, outboundMessageList.size());
        var outboundMessage = outboundMessageList.get(0);

        assertEquals(Status.DROPPED, outboundMessage.getStatus());
        assertEquals(EvaluationResultStatus.INVALID_PAYLOAD_ERROR.toString(), outboundMessage.getAdditionalFields().get("selectionFilterResult"));
        assertEquals("The payload in the incoming event was no valid JSON. Therefore, the filters could not be applied.", ((LinkedHashMap<?, ?>) outboundMessage.getAdditionalFields().get("selectionFilterTrace")).get("causeDescription"));
    }

    @Test
        // Empty payload should result in PROCESSED message when filter IS NOT set
    void emptyPayloadShouldResultInProcessedMessageWhenFilterIsNotSet() throws JsonProcessingException {
        addTestSubscription(createDefaultSubscriptionResource(TEST_ENVIRONMENT, getEventType()));

        PublishedEventMessage inboundMessage = getPublishedEventMessage("");
        simulateNewPublishedEvent(inboundMessage);

        var outboundMessageList = receiveOutboundEvents();

        assertEquals(1, outboundMessageList.size());
        var outboundMessage = outboundMessageList.get(0);

        assertEquals(Status.PROCESSED, outboundMessage.getStatus());
        assertEquals(EvaluationResultStatus.NO_FILTER.toString(), outboundMessage.getAdditionalFields().get("selectionFilterResult"));
    }

    @Test
        //Invalid JSON payload should result in DROPPED message when filter IS set
    void invalidJsonPayloadShouldResultInDroppedMessageWhenFilterIsSet() throws JsonProcessingException {
        addTestSubscription(HorizonTestHelper.getSubscriptionResourceWithSelectionFilter(TEST_ENVIRONMENT, getEventType()));

        String testEvent = """
                {
                    imInvalid,
                    "number": 12
                }""";

        PublishedEventMessage inboundMessage = getPublishedEventMessage(testEvent);
        simulateNewPublishedEvent(inboundMessage);

        var outboundMessages = receiveOutboundEvents();

        assertEquals(1, outboundMessages.size());
        var outboundMessage = outboundMessages.get(0);

        assertEquals(Status.DROPPED, outboundMessage.getStatus());
        assertEquals(EvaluationResultStatus.INVALID_PAYLOAD_ERROR.toString(), outboundMessage.getAdditionalFields().get("selectionFilterResult"));
        assertEquals("The payload in the incoming event was no valid JSON. Therefore, the filters could not be applied.", ((LinkedHashMap<?, ?>) outboundMessage.getAdditionalFields().get("selectionFilterTrace")).get("causeDescription"));
    }

    @Test
        //Invalid JSON payload should result in PROCESSED message when filter IS NOT set
    void invalidJsonPayloadShouldResultInProcessedMessageWhenFilterIsNotSet() throws JsonProcessingException {
        addTestSubscription(createDefaultSubscriptionResource(TEST_ENVIRONMENT, getEventType()));

        String testEvent = """
                {
                    imInvalid,
                    "number": 12
                }""";

        PublishedEventMessage inboundMessage = getPublishedEventMessage(testEvent);
        simulateNewPublishedEvent(inboundMessage);

        var outboundMessages = receiveOutboundEvents();

        assertEquals(1, outboundMessages.size());
        var outboundMessage = outboundMessages.get(0);

        assertEquals(Status.PROCESSED, outboundMessage.getStatus());
        assertEquals(EvaluationResultStatus.NO_FILTER.toString(), outboundMessage.getAdditionalFields().get("selectionFilterResult"));
    }
}
