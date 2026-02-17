// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.config.kafka;

import de.telekom.horizon.galaxy.kafka.KafkaConsumerHealthIndicator;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the Kafka error handler in KafkaConsumerConfig.
 * Tests the behavior when fatal exceptions occur during Kafka poll().
 */
@ExtendWith(MockitoExtension.class)
class KafkaConsumerConfigErrorHandlerTest {

    @Mock
    private Consumer<?, ?> consumer;

    @Mock
    private MessageListenerContainer container;

    @Mock
    private KafkaConsumerHealthIndicator healthIndicator;

    private CommonErrorHandler errorHandler;

    @BeforeEach
    void setUp() {
        KafkaConsumerConfig kafkaConsumerConfig = new KafkaConsumerConfig();
        errorHandler = kafkaConsumerConfig.kafkaErrorHandler(healthIndicator);
    }

    @Test
    void shouldMarkUnhealthyOnWrappedInterruptException() {
        // Given: A Kafka InterruptException wrapped in IllegalStateException
        InterruptException interruptException = new InterruptException(new InterruptedException("Test interrupt"));
        IllegalStateException wrappedException = new IllegalStateException(
                "This error handler cannot process 'InterruptException's", interruptException);

        // When: handleOtherException is called
        errorHandler.handleOtherException(wrappedException, consumer, container, false);

        // Then: Health indicator should be marked unhealthy
        verify(healthIndicator).markUnhealthy(contains("IllegalStateException"));
    }

    @Test
    void shouldMarkUnhealthyOnDirectInterruptException() {
        // Given: A direct InterruptException
        InterruptException interruptException = new InterruptException("Direct interrupt");

        // When: handleOtherException is called
        errorHandler.handleOtherException(interruptException, consumer, container, false);

        // Then: Health indicator should be marked unhealthy
        verify(healthIndicator).markUnhealthy(contains("InterruptException"));
    }

    @Test
    void shouldMarkUnhealthyOnInterruptedException() {
        // Given: A wrapped InterruptedException (not Kafka's InterruptException)
        RuntimeException wrappedException = new RuntimeException(new InterruptedException("Thread interrupted"));

        // When: handleOtherException is called
        errorHandler.handleOtherException(wrappedException, consumer, container, false);

        // Then: Health indicator should be marked unhealthy
        verify(healthIndicator).markUnhealthy(contains("RuntimeException"));
    }

    @Test
    void shouldMarkUnhealthyOnAuthenticationException() {
        // Given: An AuthenticationException (SASL/SSL auth failed)
        AuthenticationException authException = new AuthenticationException("SASL authentication failed");

        // When: handleOtherException is called
        errorHandler.handleOtherException(authException, consumer, container, false);

        // Then: Health indicator should be marked unhealthy
        verify(healthIndicator).markUnhealthy(contains("AuthenticationException"));
    }

    @Test
    void shouldMarkUnhealthyOnAuthorizationException() {
        // Given: An AuthorizationException (no ACL permissions)
        AuthorizationException authzException = new AuthorizationException("Not authorized to access topic");

        // When: handleOtherException is called
        errorHandler.handleOtherException(authzException, consumer, container, false);

        // Then: Health indicator should be marked unhealthy
        verify(healthIndicator).markUnhealthy(contains("AuthorizationException"));
    }

    @Test
    void shouldMarkUnhealthyOnFencedInstanceIdException() {
        // Given: A FencedInstanceIdException (static member fenced)
        FencedInstanceIdException fencedException = new FencedInstanceIdException("Instance was fenced");

        // When: handleOtherException is called
        errorHandler.handleOtherException(fencedException, consumer, container, false);

        // Then: Health indicator should be marked unhealthy
        verify(healthIndicator).markUnhealthy(contains("FencedInstanceIdException"));
    }

    @Test
    void shouldNotMarkUnhealthyOnOtherExceptions() {
        // Given: A non-fatal exception (TimeoutException is recoverable)
        TimeoutException timeoutException = new TimeoutException("Timeout during fetch");

        // When: handleOtherException is called
        // Note: This will call super.handleOtherException which may throw an exception
        try {
            errorHandler.handleOtherException(timeoutException, consumer, container, false);
        } catch (Exception e) {
            // Expected - default handler may throw for non-record exceptions
        }

        // Then: Health indicator should NOT be marked unhealthy
        verify(healthIndicator, never()).markUnhealthy(any());
    }
}
