// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.cache;

import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.eni.pandora.horizon.metrics.HorizonMetricsHelper;
import de.telekom.horizon.galaxy.model.SubscriptionCountGaugeCacheKey;
import io.micrometer.core.instrument.Tags;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static de.telekom.eni.pandora.horizon.metrics.HorizonMetricsConstants.*;

@Component
public class SubscriptionCountGaugeCache {

    private final HorizonMetricsHelper metricsHelper;

    private final ConcurrentHashMap<SubscriptionCountGaugeCacheKey, AtomicInteger> cache;


    public SubscriptionCountGaugeCache(HorizonMetricsHelper metricsHelper) {
        this.metricsHelper = metricsHelper;
        this.cache = new ConcurrentHashMap<>();
    }

    public AtomicInteger getOrCreateGaugeForSubscription(String environment, SubscriptionResource resource) {
        var key = buildKeyForSubscriptionResource(environment, resource);
        cache.computeIfAbsent(key, k -> createGaugeForSubscription(environment, resource));
        return cache.get(key);
    }

    private SubscriptionCountGaugeCacheKey buildKeyForSubscriptionResource(String environment, SubscriptionResource resource) {
        return new SubscriptionCountGaugeCacheKey(
                environment,
                resource.getSpec().getSubscription().getType(),
                resource.getSpec().getSubscription().getDeliveryType());
    }

    private Tags buildTagsForSubscriptionResource(String environment, SubscriptionResource resource) {
        return Tags.of(
                TAG_ENVIRONMENT, environment,
                TAG_EVENT_TYPE, resource.getSpec().getSubscription().getType(),
                TAG_DELIVERY_TYPE, resource.getSpec().getSubscription().getDeliveryType().equalsIgnoreCase("callback") ? "callback" : "server_sent_event",
                TAG_SUBSCRIBER_ID, resource.getSpec().getSubscription().getSubscriberId());
    }

    private AtomicInteger createGaugeForSubscription(String environment, SubscriptionResource resource) {
        var tags = buildTagsForSubscriptionResource(environment, resource);
        return metricsHelper.getRegistry().gauge(METRIC_SUBSCRIPTION_COUNT, tags, new AtomicInteger(0));
    }
}
