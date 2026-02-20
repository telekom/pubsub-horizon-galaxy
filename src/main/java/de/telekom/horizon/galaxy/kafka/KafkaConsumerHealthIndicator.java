// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.kafka;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.time.Instant;
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
    private final AtomicReference<Instant> errorTimestamp = new AtomicReference<>();

    /**
     * Marks the Kafka consumer as unhealthy due to a fatal exception.
     * <p>
     * Note: errorTimestamp and errorMessage are set BEFORE healthy flag to prevent
     * race conditions where health() could see healthy=false but null timestamp/message.
     *
     * @param reason Description of the failure reason
     */
    public void markUnhealthy(String reason) {
        errorTimestamp.set(Instant.now());
        errorMessage.set(reason);
        healthy.set(false);
    }

    @Override
    public Health health() {
        if (healthy.get()) {
            return Health.up().build();
        }

        Health.Builder builder = Health.down();

        String error = errorMessage.get();
        if (error != null) {
            builder.withDetail("error", error);
        }

        Instant timestamp = errorTimestamp.get();
        if (timestamp != null) {
            builder.withDetail("since", timestamp.toString());
        }

        return builder.build();
    }
}
