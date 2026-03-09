// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.service;

import de.telekom.horizon.galaxy.config.GalaxyConfig;
import de.telekom.horizon.galaxy.kafka.PublishedMessageListener;
import de.telekom.horizon.galaxy.kafka.PublishedMessageTaskFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Centralized handler for Kafka consumer backpressure.
 * <p>
 * Implements {@link RejectedExecutionHandler} so it can be registered directly on thread pool executors.
 * When a task is rejected, {@link #rejectedExecution} automatically pauses the Kafka consumer,
 * removing the need for callers to explicitly invoke {@link #pause()}.
 * <p>
 * Periodically checks whether both the batch and subscription thread pools have recovered
 * enough capacity and resumes the Kafka listener container if it was previously paused.
 */
@Component
@Slf4j
public class BackpressureHandler implements RejectedExecutionHandler {

    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final Counter pauseCounter;
    private final GalaxyConfig galaxyConfig;

    private final ConcurrentMessageListenerContainer<String, String> listenerContainer;
    private final ThreadPoolTaskExecutor batchExecutor;
    private final ThreadPoolTaskExecutor subscriptionExecutor;

    public BackpressureHandler(MeterRegistry meterRegistry, GalaxyConfig galaxyConfig, ConcurrentMessageListenerContainer<String, String> listenerContainer,
                               PublishedMessageTaskFactory publishedMessageTaskFactory, PublishedMessageListener publishedMessageListener) {
        this.galaxyConfig = galaxyConfig;

        this.listenerContainer = listenerContainer;
        this.batchExecutor = publishedMessageListener.getTaskExecutor();
        this.subscriptionExecutor = publishedMessageTaskFactory.getSubscriptionTaskExecutor();

        publishedMessageListener.getTaskExecutor().getThreadPoolExecutor().setRejectedExecutionHandler(this);
        publishedMessageTaskFactory.getSubscriptionTaskExecutor().getThreadPoolExecutor().setRejectedExecutionHandler(this);

        this.pauseCounter = Counter.builder("pubsub.kafka.listener.pause.triggered")
                .description("Number of times the Kafka listener was paused due to backpressure")
                .register(meterRegistry);
        Gauge.builder("pubsub.kafka.listener.paused", paused, p -> p.get() ? 1.0 : 0.0)
                .description("Whether the Kafka listener is currently paused due to backpressure")
                .register(meterRegistry);
    }

    /**
     * Called by the JVM when a thread pool rejects a task submission.
     * Pauses the Kafka listener container and re-throws the rejection as a
     * {@link RejectedExecutionException} so the caller can handle it.
     */
    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        pause();
        throw new RejectedExecutionException("Task " + r.toString() + " rejected from " + executor.toString());
    }

    /**
     * Pauses the Kafka listener container to stop consuming new records.
     * Safe to call from any thread; only the first call while unpaused takes effect.
     */
    public void pause() {
        if (!paused.getAndSet(true)) {
            listenerContainer.pause();
            pauseCounter.increment();
            log.info("Kafka listener container paused due to thread pool saturation.");
        }
    }

    /**
     * Resumes the Kafka listener container to continue consuming records.
     * Safe to call from any thread; only the first call while paused takes effect.
     */
    public void resume() {
        if (paused.getAndSet(false)) {
            listenerContainer.resume();
            log.info("Kafka listener container resumed after thread pool capacity recovered.");
        }
    }

    public boolean isPaused() {
        return paused.get();
    }

    /**
     * Periodically checks whether both thread pools have recovered enough capacity
     * and resumes the Kafka listener container if it was previously paused.
     */
    @Scheduled(fixedDelayString = "${galaxy.backpressure-resume-check-interval-ms}")
    public void checkAndResume() {
        if (!isPaused()) {
            return;
        }

        double batchUsage = getQueueUsageRatio(batchExecutor);
        double subscriptionUsage = getQueueUsageRatio(subscriptionExecutor);
        double threshold = galaxyConfig.getBackpressureResumeThreshold();

        if (batchUsage <= threshold && subscriptionUsage <= threshold) {
            log.info("Both thread pool queues below resume threshold ({}). Batch: {}, Subscription: {}. Resuming Kafka consumer.",
                    threshold, String.format("%.2f", batchUsage), String.format("%.2f", subscriptionUsage));
            resume();
        } else {
            log.debug("Thread pool queues still above resume threshold ({}). Batch: {}, Subscription: {}.",
                    threshold, String.format("%.2f", batchUsage), String.format("%.2f", subscriptionUsage));
        }
    }

    private double getQueueUsageRatio(ThreadPoolTaskExecutor executor) {
        var queue = executor.getThreadPoolExecutor().getQueue();
        int totalCapacity = queue.size() + queue.remainingCapacity();
        return totalCapacity > 0 ? (double) queue.size() / totalCapacity : 0.0;
    }
}
