// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.service;

import de.telekom.eni.pandora.horizon.cache.service.DeDuplicationService;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.eni.pandora.horizon.model.event.DeliveryType;
import de.telekom.eni.pandora.horizon.model.event.Event;
import de.telekom.eni.pandora.horizon.model.event.PublishedEventMessage;
import de.telekom.eni.pandora.horizon.model.event.SubscriptionEventMessage;
import de.telekom.horizon.galaxy.utils.AbstractIntegrationTest;
import de.telekom.horizon.galaxy.utils.HorizonTestHelper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class DeduplicationTests extends AbstractIntegrationTest {

    @Autowired
    private DeDuplicationService deDuplicationService;

    private final static String TEST_ENVIRONMENT = "playground";

    @Test
    void dontSendDuplicateMessages() {
        //given
        SubscriptionResource subscriptionResource = HorizonTestHelper.createDefaultSubscriptionResource(TEST_ENVIRONMENT, getEventType());

        String messageUuid = UUID.randomUUID().toString();

        Event event = new Event();
        event.setType(getEventType());
        event.setId(UUID.randomUUID().toString());

        //when
        SubscriptionEventMessage subscriptionEventMessage = new SubscriptionEventMessage(event, TEST_ENVIRONMENT, DeliveryType.CALLBACK, subscriptionResource.getSpec().getSubscription().getSubscriptionId(), messageUuid);
        deDuplicationService.track(subscriptionEventMessage);

        // then
        PublishedEventMessage publishedEventMessage = new PublishedEventMessage();
        publishedEventMessage.setUuid(messageUuid);

        assertTrue(deDuplicationService.isDuplicate(publishedEventMessage, subscriptionResource.getSpec().getSubscription().getSubscriptionId()));
    }
}
