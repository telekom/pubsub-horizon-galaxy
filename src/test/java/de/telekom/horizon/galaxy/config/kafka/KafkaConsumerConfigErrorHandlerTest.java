// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.config.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.InterruptException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the Kafka error handler in KafkaConsumerConfig.
 * Tests the behavior when fatal exceptions occur during Kafka poll().
 */
@ExtendWith(MockitoExtension.class)
class KafkaConsumerConfigErrorHandlerTest {

    @Mock
    private ApplicationContext applicationContext;

    @Mock
    private Consumer<?, ?> consumer;

    @Mock
    private MessageListenerContainer container;

    @Mock
    private KafkaConsumerHealthIndicator healthIndicator;

    private CommonErrorHandler errorHandler;

    @BeforeEach
    void setUp() {
        KafkaConsumerConfig kafkaConsumerConfig = new KafkaConsumerConfig(applicationContext);
        errorHandler = kafkaConsumerConfig.kafkaErrorHandler(healthIndicator);
    }

    @Test
    void shouldShutdownApplicationOnInterruptException() {
        // Given: An InterruptException wrapped in IllegalStateException
        InterruptException interruptException = new InterruptException(new InterruptedException("Test interrupt"));
        IllegalStateException wrappedException = new IllegalStateException(
                "This error handler cannot process 'InterruptException's", interruptException);

        try (MockedStatic<SpringApplication> springAppMock = mockStatic(SpringApplication.class)) {
            springAppMock.when(() -> SpringApplication.exit(any(), any())).thenReturn(1);

            // When: handleOtherException is called
            errorHandler.handleOtherException(wrappedException, consumer, container, false);

            // Then: Health indicator should be marked unhealthy, container stopped, and application should exit
            verify(healthIndicator).markUnhealthy(contains("IllegalStateException"));
            verify(container).stop();
            springAppMock.verify(() -> SpringApplication.exit(eq(applicationContext), any()));
        }
    }

    @Test
    void shouldShutdownApplicationOnDirectInterruptException() {
        // Given: A direct InterruptException
        InterruptException interruptException = new InterruptException("Direct interrupt");

        try (MockedStatic<SpringApplication> springAppMock = mockStatic(SpringApplication.class)) {
            springAppMock.when(() -> SpringApplication.exit(any(), any())).thenReturn(1);

            // When: handleOtherException is called
            errorHandler.handleOtherException(interruptException, consumer, container, false);

            // Then: Health indicator should be marked unhealthy, container stopped, and application should exit
            verify(healthIndicator).markUnhealthy(contains("InterruptException"));
            verify(container).stop();
            springAppMock.verify(() -> SpringApplication.exit(eq(applicationContext), any()));
        }
    }

    @Test
    void shouldShutdownApplicationOnInterruptedException() {
        // Given: A wrapped InterruptedException (not Kafka's InterruptException)
        RuntimeException wrappedException = new RuntimeException(new InterruptedException("Thread interrupted"));

        try (MockedStatic<SpringApplication> springAppMock = mockStatic(SpringApplication.class)) {
            springAppMock.when(() -> SpringApplication.exit(any(), any())).thenReturn(1);

            // When: handleOtherException is called
            errorHandler.handleOtherException(wrappedException, consumer, container, false);

            // Then: Health indicator should be marked unhealthy, container stopped, and application should exit
            verify(healthIndicator).markUnhealthy(contains("RuntimeException"));
            verify(container).stop();
            springAppMock.verify(() -> SpringApplication.exit(eq(applicationContext), any()));
        }
    }

    @Test
    void shouldShutdownApplicationOnAuthenticationException() {
        // Given: An AuthenticationException (SASL/SSL auth failed)
        AuthenticationException authException = new AuthenticationException("SASL authentication failed");

        try (MockedStatic<SpringApplication> springAppMock = mockStatic(SpringApplication.class)) {
            springAppMock.when(() -> SpringApplication.exit(any(), any())).thenReturn(1);

            // When: handleOtherException is called
            errorHandler.handleOtherException(authException, consumer, container, false);

            // Then: Health indicator should be marked unhealthy, container stopped, and application should exit
            verify(healthIndicator).markUnhealthy(contains("AuthenticationException"));
            verify(container).stop();
            springAppMock.verify(() -> SpringApplication.exit(eq(applicationContext), any()));
        }
    }

    @Test
    void shouldShutdownApplicationOnAuthorizationException() {
        // Given: An AuthorizationException (no ACL permissions)
        AuthorizationException authzException = new AuthorizationException("Not authorized to access topic");

        try (MockedStatic<SpringApplication> springAppMock = mockStatic(SpringApplication.class)) {
            springAppMock.when(() -> SpringApplication.exit(any(), any())).thenReturn(1);

            // When: handleOtherException is called
            errorHandler.handleOtherException(authzException, consumer, container, false);

            // Then: Health indicator should be marked unhealthy, container stopped, and application should exit
            verify(healthIndicator).markUnhealthy(contains("AuthorizationException"));
            verify(container).stop();
            springAppMock.verify(() -> SpringApplication.exit(eq(applicationContext), any()));
        }
    }

    @Test
    void shouldShutdownApplicationOnFencedInstanceIdException() {
        // Given: A FencedInstanceIdException (static member fenced)
        FencedInstanceIdException fencedException = new FencedInstanceIdException("Instance was fenced");

        try (MockedStatic<SpringApplication> springAppMock = mockStatic(SpringApplication.class)) {
            springAppMock.when(() -> SpringApplication.exit(any(), any())).thenReturn(1);

            // When: handleOtherException is called
            errorHandler.handleOtherException(fencedException, consumer, container, false);

            // Then: Health indicator should be marked unhealthy, container stopped, and application should exit
            verify(healthIndicator).markUnhealthy(contains("FencedInstanceIdException"));
            verify(container).stop();
            springAppMock.verify(() -> SpringApplication.exit(eq(applicationContext), any()));
        }
    }

    @Test
    void shouldShutdownApplicationOnIllegalStateException() {
        // Given: An IllegalStateException (consumer in invalid state)
        IllegalStateException illegalStateException = new IllegalStateException("Consumer is not subscribed");

        try (MockedStatic<SpringApplication> springAppMock = mockStatic(SpringApplication.class)) {
            springAppMock.when(() -> SpringApplication.exit(any(), any())).thenReturn(1);

            // When: handleOtherException is called
            errorHandler.handleOtherException(illegalStateException, consumer, container, false);

            // Then: Health indicator should be marked unhealthy, container stopped, and application should exit
            verify(healthIndicator).markUnhealthy(contains("IllegalStateException"));
            verify(container).stop();
            springAppMock.verify(() -> SpringApplication.exit(eq(applicationContext), any()));
        }
    }

    @Test
    void shouldNotShutdownOnOtherExceptions() {
        // Given: A non-fatal exception (TimeoutException is recoverable)
        org.apache.kafka.common.errors.TimeoutException timeoutException =
            new org.apache.kafka.common.errors.TimeoutException("Timeout during fetch");

        try (MockedStatic<SpringApplication> springAppMock = mockStatic(SpringApplication.class)) {
            // When: handleOtherException is called
            // Note: This will call super.handleOtherException which may throw, so we catch it
            try {
                errorHandler.handleOtherException(timeoutException, consumer, container, false);
            } catch (Exception e) {
                // Expected - default handler may throw for non-record exceptions
            }

            // Then: Health indicator should NOT be marked unhealthy, container should NOT be stopped
            verify(healthIndicator, never()).markUnhealthy(any());
            verify(container, never()).stop();
            springAppMock.verify(() -> SpringApplication.exit(any(), any()), never());
        }
    }
}
