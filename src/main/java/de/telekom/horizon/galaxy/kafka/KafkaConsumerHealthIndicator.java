// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.kafka;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Health indicator for the Kafka consumer.
 * Reports unhealthy status when a fatal Kafka exception occurs.
 */
@Component
public class KafkaConsumerHealthIndicator implements HealthIndicator {

    private final AtomicBoolean healthy = new AtomicBoolean(true);
    private final AtomicReference<String> errorMessage = new AtomicReference<>();

    /**
     * Marks the Kafka consumer as unhealthy due to a fatal exception.
     *
     * @param reason Description of the failure reason
     */
    public void markUnhealthy(String reason) {
        healthy.set(false);
        errorMessage.set(reason);
    }

    @Override
    public Health health() {
        if (healthy.get()) {
            return Health.up().build();
        }
        return Health.down()
                .withDetail("error", errorMessage.get())
                .build();
    }
}
