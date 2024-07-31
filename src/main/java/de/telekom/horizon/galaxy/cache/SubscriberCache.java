// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.cache;

import de.telekom.eni.pandora.horizon.cache.service.JsonCacheService;
import de.telekom.eni.pandora.horizon.cache.util.Query;
import de.telekom.eni.pandora.horizon.exception.JsonCacheException;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.horizon.galaxy.config.GalaxyConfig;
import de.telekom.horizon.galaxy.model.SubscriptionCacheKey;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The {@code SubscriptionCache} class is responsible for caching {@link SubscriptionResource} instances
 * based on the environment and event type. Additionally, it interacts with a {@link SubscriptionCountGaugeCache} to maintain
 * subscription count gauges.
 */
@Slf4j
@Component
public class SubscriberCache {

    private final SubscriptionCountGaugeCache subscriptionCountGaugeCache;

    private final JsonCacheService<SubscriptionResource> subscriptionCache;

    private final GalaxyConfig galaxyConfig;

    public SubscriberCache(SubscriptionCountGaugeCache subscriptionCountGaugeCache, JsonCacheService<SubscriptionResource> subscriptionCache, GalaxyConfig galaxyConfig) {
        this.subscriptionCountGaugeCache = subscriptionCountGaugeCache;
        this.subscriptionCache = subscriptionCache;
        this.galaxyConfig = galaxyConfig;
    }

    private SubscriptionCacheKey generateSubscriptionCacheKey(String environment, String eventType) {
        return new SubscriptionCacheKey(environment, eventType);
    }

    /**
     * Retrieves a map of {@link SubscriptionResource} for the specified environment and event type.
     *
     * @param environment   The environment to gather subscriptions from.
     * @param eventType     The type of event for the subscriptions.
     * @return A  where the keys are the SubscriptionIds and the values are the corresponding {@link SubscriptionResource}.
     * If no such subscriptions exist for the given environment and event type, this method may return <strong>null</strong>.
     */
    public List<SubscriptionResource> getSubscriptionsForEnvironmentAndEventType(String environment, String eventType) {

        var env = environment;
        if (Objects.equals(galaxyConfig.getDefaultEnvironment(), environment)) {
            env = "default";
        }

        var builder = Query.builder(SubscriptionResource.class)
                .addMatcher("spec.environment", env)
                .addMatcher("spec.subscription.type", eventType);

        List<SubscriptionResource> list = new ArrayList<>();
        try {
            list = subscriptionCache.getQuery(builder.build());

        } catch (JsonCacheException e) {
            log.error("Error occurred while executing query on JsonCacheService", e);
        }
        return list;
    }
}
