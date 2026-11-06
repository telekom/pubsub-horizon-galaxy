// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.cache;

import de.telekom.eni.pandora.horizon.cache.service.SubscriptionCacheReader;
import de.telekom.eni.pandora.horizon.exception.JsonCacheException;
import de.telekom.eni.pandora.horizon.exception.SubscriptionCacheReadException;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.horizon.galaxy.config.GalaxyConfig;
import de.telekom.horizon.galaxy.model.SubscriptionCacheKey;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

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

    private final SubscriptionCacheReader subscriptionCache;

    private final GalaxyConfig galaxyConfig;

    public SubscriberCache(SubscriptionCountGaugeCache subscriptionCountGaugeCache,
                           @Qualifier("subscriptionCacheReader") SubscriptionCacheReader subscriptionCache,
                           GalaxyConfig galaxyConfig) {
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
     * If no such subscriptions exist for the given environment and event type, this method returns an empty list.
     * @throws SubscriptionLookupException if the subscriptions cannot be read; the event must be re-consumed
     */
    public List<SubscriptionResource> getSubscriptionsForEnvironmentAndEventType(String environment, String eventType) {

        var env = environment;
        if (Objects.equals(galaxyConfig.getDefaultEnvironment(), environment)) {
            env = "default";
        }

        try {
            return subscriptionCache.findByEnvironmentAndEventType(env, eventType);
        } catch (SubscriptionCacheReadException exception) {
            // Handle JSON mapping errors of fallback (JsonCacheException) as before; log and return an empty list
            if (exception.getCause() instanceof JsonCacheException) {
                log.error("Error occurred while executing query on JsonCacheService for environment {} and event type {}",
                        env, eventType, exception);
                return List.of();
            }
            // Otherwise nack the message by throwing an exception
            throw new SubscriptionLookupException("Error occurred while reading subscriptions for environment "
                    + env + " and event type " + eventType, exception);
        }
    }
}
