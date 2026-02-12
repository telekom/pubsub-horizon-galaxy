// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.config.kafka;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for KafkaConsumerHealthIndicator.
 */
class KafkaConsumerHealthIndicatorTest {

    private KafkaConsumerHealthIndicator healthIndicator;

    @BeforeEach
    void setUp() {
        healthIndicator = new KafkaConsumerHealthIndicator();
    }

    @Test
    void shouldBeHealthyByDefault() {
        // When
        Health health = healthIndicator.health();

        // Then
        assertEquals(Status.UP, health.getStatus());
        assertNull(health.getDetails().get("error"));
    }

    @Test
    void shouldBeUnhealthyAfterMarkUnhealthy() {
        // Given
        String reason = "Fatal Kafka exception: AuthenticationException - SASL authentication failed";

        // When
        healthIndicator.markUnhealthy(reason);
        Health health = healthIndicator.health();

        // Then
        assertEquals(Status.DOWN, health.getStatus());
        assertEquals(reason, health.getDetails().get("error"));
    }

    @Test
    void shouldRemainUnhealthyAfterMultipleCalls() {
        // Given
        healthIndicator.markUnhealthy("First error");

        // When
        healthIndicator.markUnhealthy("Second error");
        Health health = healthIndicator.health();

        // Then
        assertEquals(Status.DOWN, health.getStatus());
        assertEquals("Second error", health.getDetails().get("error"));
    }
}
