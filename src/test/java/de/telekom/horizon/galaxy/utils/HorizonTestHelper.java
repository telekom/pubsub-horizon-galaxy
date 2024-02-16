// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.utils;

import de.telekom.eni.jfilter.operator.comparison.EqualsOperator;
import de.telekom.eni.jfilter.operator.comparison.GreaterEqualOperator;
import de.telekom.eni.jfilter.operator.logic.AndOperator;
import de.telekom.eni.pandora.horizon.kubernetes.resource.Subscription;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResourceSpec;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionTrigger;

import java.util.List;
import java.util.UUID;

public class HorizonTestHelper {

    public static SubscriptionResource createDefaultSubscriptionResource(final String environment, final String eventType) {
        var subscriptionRes = new SubscriptionResource();
        var spec = new SubscriptionResourceSpec();
        spec.setEnvironment(environment);
        var subscription = new Subscription();
        subscription.setSubscriptionId(UUID.randomUUID().toString());
        subscription.setSubscriberId(UUID.randomUUID().toString());
        subscription.setCallback("https://localhost:4711/foobar");
        subscription.setType(eventType);
        subscription.setDeliveryType("callback");
        subscription.setPayloadType("data");
        subscription.setPublisherId(UUID.randomUUID().toString());
        spec.setSubscription(subscription);

        subscriptionRes.setSpec(spec);

        return subscriptionRes;
    }

    public static SubscriptionResource createDefaultSubscriptionResourceWithSubscriberId(final String environment, final String eventType, String subscriberId) {
        SubscriptionResource resource = createDefaultSubscriptionResource(environment, eventType);
        resource.getSpec().getSubscription().setSubscriberId(subscriberId);
        return resource;
    }

    public static SubscriptionResource getSubscriptionResourceWithSelectionFilter(final String environment, final String eventType) {
        var subscriptionResource = createDefaultSubscriptionResource(environment, eventType);
        var subscription = subscriptionResource.getSpec().getSubscription();

        var filterOperator = new AndOperator(List.of(
                new EqualsOperator<String>("$.foo", "bar"),
                new GreaterEqualOperator<Integer>("$.number", 12)
        ));

        var subscriptionTrigger = new SubscriptionTrigger();
        subscriptionTrigger.setAdvancedSelectionFilter(filterOperator);
        subscription.setTrigger(subscriptionTrigger);

        subscriptionResource.getSpec().setSubscription(subscription);

        return subscriptionResource;
    }

    public static SubscriptionResource getSubscriptionResourceWithResponseFilter(final String environment, final String eventType) {
        var subscriptionResource = createDefaultSubscriptionResource(environment, eventType);
        var subscription = subscriptionResource.getSpec().getSubscription();

        var publisherTrigger = new SubscriptionTrigger();
        publisherTrigger.setResponseFilter(List.of("foo.foo", "foo.bar"));

        var subscriptionTrigger = new SubscriptionTrigger();
        subscriptionTrigger.setResponseFilter(List.of("bar.bar", "foo.bar"));
        subscription.setPublisherTrigger(publisherTrigger);
        subscription.setTrigger(subscriptionTrigger);

        subscriptionResource.getSpec().setSubscription(subscription);

        return subscriptionResource;
    }

    public static SubscriptionResource createDefaultSubscriptionResourceWithSubscriberId(String testEnvironment, String eventType, String retentionTime, String subscriber) {
        var subscriptionResource = createDefaultSubscriptionResourceWithSubscriberId(testEnvironment, eventType, subscriber);
        subscriptionResource.getSpec().getSubscription().setEventRetentionTime(retentionTime);

        return subscriptionResource;
    }
}
