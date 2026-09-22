// Copyright 2026 Deutsche Telekom AG
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.cache;

import de.telekom.eni.pandora.horizon.cache.service.SubscriptionCacheReader;
import de.telekom.eni.pandora.horizon.exception.SubscriptionCacheReadException;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.horizon.galaxy.config.GalaxyConfig;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SubscriberCacheTest {

    private final SubscriptionCountGaugeCache subscriptionCountGaugeCache = mock(SubscriptionCountGaugeCache.class);
    private final SubscriptionCacheReader subscriptionCache = mock(SubscriptionCacheReader.class);
    private final GalaxyConfig galaxyConfig = mock(GalaxyConfig.class);
    private final SubscriberCache subscriberCache =
            new SubscriberCache(subscriptionCountGaugeCache, subscriptionCache, galaxyConfig);

    @Test
    void shouldReadSubscriptionsByEnvironmentAndEventType() throws SubscriptionCacheReadException {
        var expected = List.of(new SubscriptionResource());
        when(subscriptionCache.findByEnvironmentAndEventType("production", "event-type")).thenReturn(expected);

        var result = subscriberCache.getSubscriptionsForEnvironmentAndEventType("production", "event-type");

        assertSame(expected, result);
    }

    @Test
    void shouldMapConfiguredDefaultEnvironment() throws SubscriptionCacheReadException {
        when(galaxyConfig.getDefaultEnvironment()).thenReturn("playground");

        subscriberCache.getSubscriptionsForEnvironmentAndEventType("playground", "event-type");

        verify(subscriptionCache).findByEnvironmentAndEventType("default", "event-type");
    }

    @Test
    void shouldReturnEmptyListWhenSubscriptionCacheReadFails() throws SubscriptionCacheReadException {
        when(subscriptionCache.findByEnvironmentAndEventType("production", "event-type"))
                .thenThrow(new SubscriptionCacheReadException("cache unavailable"));

        var result = subscriberCache.getSubscriptionsForEnvironmentAndEventType("production", "event-type");

        assertEquals(List.of(), result);
    }
}