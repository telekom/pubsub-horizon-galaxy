// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.kubernetes;

import de.telekom.eni.pandora.horizon.kubernetes.HorizonResourceEventHandler;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.horizon.galaxy.cache.SubscriptionCache;
import de.telekom.horizon.galaxy.config.GalaxyConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Optional;

@Service
@Slf4j
public class SubscriptionResourceEventHandler implements HorizonResourceEventHandler<SubscriptionResource> {

    private final SubscriptionCache subscriptionCache;
    private final GalaxyConfig galaxyConfig;

    @Autowired
    public SubscriptionResourceEventHandler(SubscriptionCache subscriptionCache, GalaxyConfig galaxyConfig) {
        this.subscriptionCache = subscriptionCache;
        this.galaxyConfig = galaxyConfig;
    }

    @Override
    public void onAdd(SubscriptionResource resource) {
        log.debug("Add: {}", resource);

        var subscription = resource.getSpec().getSubscription();

        subscriptionCache.add(determineEnvironment(resource), subscription.getType(), resource);
    }

    @Override
    public void onUpdate(SubscriptionResource oldResource, SubscriptionResource newResource) {
        log.debug("Update: {}", newResource);

        var newSubscription = newResource.getSpec().getSubscription();

        subscriptionCache.add(determineEnvironment(newResource), newSubscription.getType(), newResource);
    }

    @Override
    public void onDelete(SubscriptionResource resource, boolean deletedFinalStateUnknown) {
        log.debug("Delete: {}", resource);

        var subscription = resource.getSpec().getSubscription();

        subscriptionCache.remove(determineEnvironment(resource), subscription.getType(), resource);
    }

    private String determineEnvironment(SubscriptionResource resource) {
        return Optional.ofNullable(resource.getSpec().getEnvironment()).orElse(galaxyConfig.getDefaultEnvironment());
    }

    @Override
    public void onInitialStateSet(Collection<SubscriptionResource> collection) {
        collection.forEach(l -> onAdd(l));
    }
}
