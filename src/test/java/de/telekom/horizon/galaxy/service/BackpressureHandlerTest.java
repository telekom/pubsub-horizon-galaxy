// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.service;

import de.telekom.horizon.galaxy.config.GalaxyConfig;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class BackpressureHandlerTest {

    private MeterRegistry meterRegistry;
    private BackpressureHandler handler;

    @SuppressWarnings("unchecked")
    private final ConcurrentMessageListenerContainer<String, String> container = mock(ConcurrentMessageListenerContainer.class);

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        var galaxyConfig = new GalaxyConfig();
        galaxyConfig.setBackpressureResumeThreshold(0.5);
        handler = new BackpressureHandler(meterRegistry, galaxyConfig);
    }

    @Test
    void pauseShouldPauseContainerAndSetState() {
        handler.setListenerContainer(container);

        handler.pause();

        assertTrue(handler.isPaused());
        verify(container).pause();
        assertEquals(1.0, meterRegistry.get("pubsub.kafka.listener.pause.triggered").counter().count());
        assertEquals(1.0, meterRegistry.get("pubsub.kafka.listener.paused").gauge().value());
    }

    @Test
    void pauseCalledTwiceShouldOnlyPauseOnce() {
        handler.setListenerContainer(container);

        handler.pause();
        handler.pause();

        verify(container, times(1)).pause();
        assertEquals(1.0, meterRegistry.get("pubsub.kafka.listener.pause.triggered").counter().count());
    }

    @Test
    void resumeShouldResumeContainerAndClearState() {
        handler.setListenerContainer(container);
        handler.pause();

        handler.resume();

        assertFalse(handler.isPaused());
        verify(container).resume();
        assertEquals(0.0, meterRegistry.get("pubsub.kafka.listener.paused").gauge().value());
    }

    @Test
    void resumeCalledTwiceShouldOnlyResumeOnce() {
        handler.setListenerContainer(container);
        handler.pause();

        handler.resume();
        handler.resume();

        verify(container, times(1)).resume();
    }

    @Test
    void resumeWithoutPauseShouldDoNothing() {
        handler.setListenerContainer(container);

        handler.resume();

        assertFalse(handler.isPaused());
        verify(container, never()).resume();
    }

    @Test
    void pauseWithoutContainerShouldNotThrow() {
        // No container set
        assertDoesNotThrow(() -> handler.pause());
        assertFalse(handler.isPaused());
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
        handler.setListenerContainer(container);

        assertEquals(0.0, meterRegistry.get("pubsub.kafka.listener.paused").gauge().value());

        handler.pause();
        assertEquals(1.0, meterRegistry.get("pubsub.kafka.listener.paused").gauge().value());

        handler.resume();
        assertEquals(0.0, meterRegistry.get("pubsub.kafka.listener.paused").gauge().value());
    }

    @Test
    void rejectedExecutionShouldPauseAndThrow() {
        handler.setListenerContainer(container);
        var runnable = mock(Runnable.class);
        var executor = mock(ThreadPoolExecutor.class);

        assertThrows(RejectedExecutionException.class, () -> handler.rejectedExecution(runnable, executor));
        assertTrue(handler.isPaused());
        verify(container).pause();
    }

    @Test
    void checkAndResumeShouldSkipWhenNotPaused() {
        handler.setListenerContainer(container);

        handler.checkAndResume();

        verify(container, never()).resume();
    }

    @Test
    void checkAndResumeShouldResumeWhenBothPoolsBelowThreshold() {
        handler.setListenerContainer(container);
        handler.pause();

        var batchExecutor = createExecutorWithQueueUsage(0.3);
        var subscriptionExecutor = createExecutorWithQueueUsage(0.2);
        handler.setBatchExecutor(batchExecutor);
        handler.setSubscriptionExecutor(subscriptionExecutor);

        handler.checkAndResume();

        assertFalse(handler.isPaused());
        verify(container).resume();
    }

    @Test
    void checkAndResumeShouldNotResumeWhenBatchPoolAboveThreshold() {
        handler.setListenerContainer(container);
        handler.pause();

        var batchExecutor = createExecutorWithQueueUsage(0.8);
        var subscriptionExecutor = createExecutorWithQueueUsage(0.2);
        handler.setBatchExecutor(batchExecutor);
        handler.setSubscriptionExecutor(subscriptionExecutor);

        handler.checkAndResume();

        assertTrue(handler.isPaused());
        verify(container, never()).resume();
    }

    @Test
    void checkAndResumeShouldNotResumeWhenSubscriptionPoolAboveThreshold() {
        handler.setListenerContainer(container);
        handler.pause();

        var batchExecutor = createExecutorWithQueueUsage(0.3);
        var subscriptionExecutor = createExecutorWithQueueUsage(0.7);
        handler.setBatchExecutor(batchExecutor);
        handler.setSubscriptionExecutor(subscriptionExecutor);

        handler.checkAndResume();

        assertTrue(handler.isPaused());
        verify(container, never()).resume();
    }

    @Test
    void checkAndResumeShouldResumeWhenBothPoolsExactlyAtThreshold() {
        handler.setListenerContainer(container);
        handler.pause();

        var batchExecutor = createExecutorWithQueueUsage(0.5);
        var subscriptionExecutor = createExecutorWithQueueUsage(0.5);
        handler.setBatchExecutor(batchExecutor);
        handler.setSubscriptionExecutor(subscriptionExecutor);

        handler.checkAndResume();

        assertFalse(handler.isPaused());
        verify(container).resume();
    }

    @Test
    void checkAndResumeShouldHandleNullExecutors() {
        handler.setListenerContainer(container);
        handler.pause();
        // Don't set any executors — they remain null

        handler.checkAndResume();

        // Null executors return 0.0 usage, which is below threshold → resume
        assertFalse(handler.isPaused());
        verify(container).resume();
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
                try { Thread.sleep(60000); } catch (InterruptedException ignored) {}
            });
        }

        return executor;
    }
}
