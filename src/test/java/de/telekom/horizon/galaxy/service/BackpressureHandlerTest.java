// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.service;

import de.telekom.horizon.galaxy.config.GalaxyConfig;
import de.telekom.horizon.galaxy.kafka.PublishedMessageListener;
import de.telekom.horizon.galaxy.kafka.PublishedMessageTaskFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class BackpressureHandlerTest {

    private MeterRegistry meterRegistry;
    private BackpressureHandler handler;

    GalaxyConfig galaxyConfig = new GalaxyConfig();

    @SuppressWarnings("unchecked")
    private final ConcurrentMessageListenerContainer<String, String> container = mock(ConcurrentMessageListenerContainer.class);

    private final PublishedMessageTaskFactory publishedMessageTaskFactory = mock(PublishedMessageTaskFactory.class);
    private final PublishedMessageListener publishedMessageListener = mock(PublishedMessageListener.class);

    private final ThreadPoolTaskExecutor batchExecutor = createExecutorWithQueueUsage(0.3);
    private final ThreadPoolTaskExecutor subscriptionExecutor = createExecutorWithQueueUsage(0.7);

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        galaxyConfig.setBackpressureResumeThreshold(0.5);

        when(publishedMessageListener.getTaskExecutor()).thenReturn(batchExecutor);
        when(publishedMessageTaskFactory.getSubscriptionTaskExecutor()).thenReturn(subscriptionExecutor);

        handler = new BackpressureHandler(meterRegistry, galaxyConfig, container, publishedMessageTaskFactory, publishedMessageListener);
    }

    @Test
    void pauseShouldPauseContainerAndSetState() {
        handler.pause();

        assertTrue(handler.isPaused());
        verify(container).pause();
        assertEquals(1.0, meterRegistry.get("pubsub.kafka.listener.pause.triggered").counter().count());
        assertEquals(1.0, meterRegistry.get("pubsub.kafka.listener.paused").gauge().value());
    }

    @Test
    void pauseCalledTwiceShouldOnlyPauseOnce() {
        handler.pause();
        handler.pause();

        verify(container, times(1)).pause();
        assertEquals(1.0, meterRegistry.get("pubsub.kafka.listener.pause.triggered").counter().count());
    }

    @Test
    void resumeShouldResumeContainerAndClearState() {
        handler.pause();

        handler.resume();

        assertFalse(handler.isPaused());
        verify(container).resume();
        assertEquals(0.0, meterRegistry.get("pubsub.kafka.listener.paused").gauge().value());
    }

    @Test
    void resumeCalledTwiceShouldOnlyResumeOnce() {
        handler.pause();

        handler.resume();
        handler.resume();

        verify(container, times(1)).resume();
    }

    @Test
    void resumeWithoutPauseShouldDoNothing() {
        handler.resume();

        assertFalse(handler.isPaused());
        verify(container, never()).resume();
    }

    @Test
    void resumeWithoutContainerShouldNotThrow() {
        // No container set
        assertDoesNotThrow(() -> handler.resume());
        assertFalse(handler.isPaused());
    }

    @Test
    void isPausedShouldReturnFalseInitially() {
        assertFalse(handler.isPaused());
    }

    @Test
    void gaugeReflectsPausedState() {
        assertEquals(0.0, meterRegistry.get("pubsub.kafka.listener.paused").gauge().value());

        handler.pause();
        assertEquals(1.0, meterRegistry.get("pubsub.kafka.listener.paused").gauge().value());

        handler.resume();
        assertEquals(0.0, meterRegistry.get("pubsub.kafka.listener.paused").gauge().value());
    }

    @Test
    void rejectedExecutionShouldPauseAndThrow() {
        var runnable = mock(Runnable.class);
        var executor = mock(ThreadPoolExecutor.class);

        assertThrows(RejectedExecutionException.class, () -> handler.rejectedExecution(runnable, executor));
        assertTrue(handler.isPaused());
        verify(container).pause();
    }

    @Test
    void checkAndResumeShouldSkipWhenNotPaused() {
        handler.checkAndResume();

        verify(container, never()).resume();
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource({
            "Resume when both pools below threshold, 0.3, 0.2, true",   // Both below threshold
            "Do not resume when batch-pool above threshold, 0.8, 0.2, false",  // Batch above threshold
            "Do not resume when subscription-pool above threshold, 0.3, 0.7, false",  // Subscription above threshold
            "Resume when both pools exactly at threshold, 0.5, 0.5, true"    // Both exactly at threshold
    })
    void checkAndResumeShouldHandleThresholds(String displayName, double batchUsage, double subscriptionUsage, boolean shouldResume) {
        var newBatchExecutor = createExecutorWithQueueUsage(batchUsage);
        var newSubscriptionExecutor = createExecutorWithQueueUsage(subscriptionUsage);

        when(publishedMessageListener.getTaskExecutor()).thenReturn(newBatchExecutor);
        when(publishedMessageTaskFactory.getSubscriptionTaskExecutor()).thenReturn(newSubscriptionExecutor);

        handler = new BackpressureHandler(meterRegistry, galaxyConfig, container, publishedMessageTaskFactory, publishedMessageListener);

        handler.pause();

        handler.checkAndResume();

        if (shouldResume) {
            assertFalse(handler.isPaused());
            verify(container).resume();
        } else {
            assertTrue(handler.isPaused());
            verify(container, never()).resume();
        }
    }

    private ThreadPoolTaskExecutor createExecutorWithQueueUsage(double ratio) {
        // Create a real executor to avoid complex mocking of getThreadPoolExecutor().getQueue()
        int capacity = 100;
        int queuedItems = (int) (capacity * ratio);

        var executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(1);
        executor.setQueueCapacity(capacity);
        executor.afterPropertiesSet();

        // Fill queue to desired ratio
        for (int i = 0; i < queuedItems; i++) {
            executor.getThreadPoolExecutor().getQueue().add(() -> {
                try {
                    Thread.sleep(60000);
                } catch (InterruptedException ignored) {
                }
            });
        }

        return executor;
    }
}
