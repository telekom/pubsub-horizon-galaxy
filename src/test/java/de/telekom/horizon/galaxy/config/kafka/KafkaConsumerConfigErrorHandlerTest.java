// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.config.kafka;

import org.apache.kafka.clients.consumer.Consumer;
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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the Kafka error handler in KafkaConsumerConfig.
 * Tests the behavior when InterruptException occurs during Kafka poll().
 */
@ExtendWith(MockitoExtension.class)
class KafkaConsumerConfigErrorHandlerTest {

    @Mock
    private ApplicationContext applicationContext;

    @Mock
    private Consumer<?, ?> consumer;

    @Mock
    private MessageListenerContainer container;

    private KafkaConsumerConfig kafkaConsumerConfig;
    private CommonErrorHandler errorHandler;

    @BeforeEach
    void setUp() {
        kafkaConsumerConfig = new KafkaConsumerConfig(applicationContext);
        errorHandler = kafkaConsumerConfig.kafkaErrorHandler();
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

            // Then: Container should be stopped and application should exit
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

            // Then: Container should be stopped and application should exit
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

            // Then: Container should be stopped and application should exit
            verify(container).stop();
            springAppMock.verify(() -> SpringApplication.exit(eq(applicationContext), any()));
        }
    }

    @Test
    void shouldNotShutdownOnOtherExceptions() {
        // Given: A non-interrupt exception
        RuntimeException otherException = new RuntimeException("Some other error");

        try (MockedStatic<SpringApplication> springAppMock = mockStatic(SpringApplication.class)) {
            // When: handleOtherException is called
            // Note: This will call super.handleOtherException which may throw, so we catch it
            try {
                errorHandler.handleOtherException(otherException, consumer, container, false);
            } catch (Exception e) {
                // Expected - default handler may throw for non-record exceptions
            }

            // Then: Container should NOT be stopped by our handler
            verify(container, never()).stop();
            springAppMock.verify(() -> SpringApplication.exit(any(), any()), never());
        }
    }
}
