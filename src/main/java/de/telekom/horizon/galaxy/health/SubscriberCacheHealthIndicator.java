// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.health;

import de.telekom.eni.pandora.horizon.kubernetes.InformerStoreInitHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * This component implements {@link HealthIndicator} to monitor health status of subscriber cache in kubernetes.
 * The health status is determined by whether the {@link InformerStoreInitHandler} is fully synced or not.
 * Only enabled when the {@code kubernetes.enabled} property is set to {@code true} in the configuration.
 */
@Component
@ConditionalOnProperty(value = "kubernetes.enabled", havingValue = "true")
public class SubscriberCacheHealthIndicator implements HealthIndicator {
    private final InformerStoreInitHandler informerStoreInitHandler;

    public SubscriberCacheHealthIndicator(@Autowired(required = false) InformerStoreInitHandler informerStoreInitHandler) {
        this.informerStoreInitHandler = informerStoreInitHandler;
    }

    /**
     * Checks the health of the subscriber cache. If {@link InformerStoreInitHandler} is fully synced,
     * the status will be {@code UP}, otherwise it will be {@code DOWN}. The status detail would include
     * initial sync stats of the {@link InformerStoreInitHandler}.
     *
     * @return the Health status of the subscriber cache
     */
    @Override
    public Health health() {
        Health.Builder status = Health.up();

        if (!informerStoreInitHandler.isFullySynced()) {
            status = Health.down();
        }

        return status.withDetails(informerStoreInitHandler.getInitalSyncedStats()).build();
    }
}