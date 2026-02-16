// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.kafka;

import de.telekom.eni.pandora.horizon.model.event.PublishedEventMessage;
import de.telekom.eni.pandora.horizon.tracing.HorizonTracer;
import de.telekom.horizon.galaxy.config.GalaxyConfig;
import de.telekom.horizon.galaxy.model.PublishedMessageTaskResult;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jetbrains.annotations.NotNull;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * The {@code PublishedMessageListener} class is responsible for processing Kafka messages in batches.
 * <p>
 * The class handles new {@link ConsumerRecord} objects containing {@link PublishedEventMessage} objects.
 * For each {@link ConsumerRecord} a task is created using the {@link PublishedMessageTaskFactory}.
 * The result of each task is collected in a list of {@link Future} objects.
 * If any task in the list fails, a nack (negative acknowledgment) for the failed message
 * and all following messages in the batch is sent to Kafka. All messages before the failed message are getting acknowledged.
 * If all tasks are successful, an acknowledgment for the batch is sent to Kafka.
 */
@Slf4j
public class PublishedMessageListener extends AbstractConsumerSeekAware implements BatchAcknowledgingMessageListener<String, String> {

    private final PublishedMessageTaskFactory publishedMessageTaskFactory;
    private final ThreadPoolTaskExecutor taskExecutor;
    private final HorizonTracer tracer;
    private final GalaxyConfig galaxyConfig;
    private final Counter threadPoolSaturatedCounter;
    private final Counter nackCounter;
    private final Counter nackDueToRejectionCounter;
    private final Counter nackDueToTaskFailureCounter;


    public PublishedMessageListener(PublishedMessageTaskFactory publishedMessageTaskFactory, HorizonTracer horizonTracer, GalaxyConfig galaxyConfig, MeterRegistry meterRegistry) {
        super();
        this.publishedMessageTaskFactory = publishedMessageTaskFactory;
        this.tracer = horizonTracer;
        this.galaxyConfig = galaxyConfig;

        this.taskExecutor = initThreadPoolTaskExecutor(galaxyConfig, meterRegistry);
        this.threadPoolSaturatedCounter = Counter.builder("galaxy.batch.threadpool.saturated")
                .description("Number of times the batch thread pool rejected tasks due to queue saturation")
                .register(meterRegistry);
        this.nackCounter = Counter.builder("galaxy.kafka.listener.nacks")
                .description("Total number of batch nacks")
                .register(meterRegistry);
        this.nackDueToRejectionCounter = Counter.builder("galaxy.kafka.listener.nacks.rejection")
                .description("Nacks due to thread pool rejection")
                .register(meterRegistry);
        this.nackDueToTaskFailureCounter = Counter.builder("galaxy.kafka.listener.nacks.task_failure")
                .description("Nacks due to task execution failure")
                .register(meterRegistry);
    }

    @NotNull
    private ThreadPoolTaskExecutor initThreadPoolTaskExecutor(GalaxyConfig galaxyConfig, MeterRegistry meterRegistry) {
        final ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setCorePoolSize(galaxyConfig.getBatchCoreThreadPoolSize());
        threadPoolTaskExecutor.setMaxPoolSize(galaxyConfig.getBatchMaxThreadPoolSize());
        threadPoolTaskExecutor.setQueueCapacity(galaxyConfig.getBatchQueueCapacity());
        threadPoolTaskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        threadPoolTaskExecutor.setPrestartAllCoreThreads(true);
        threadPoolTaskExecutor.setThreadGroupName("batch");
        threadPoolTaskExecutor.setThreadNamePrefix("batch-");
        threadPoolTaskExecutor.afterPropertiesSet();
        
        // Register metrics for the thread pool
        ExecutorServiceMetrics.monitor(meterRegistry, threadPoolTaskExecutor.getThreadPoolExecutor(), 
            "batchTaskExecutor", Collections.emptyList());
        
        return threadPoolTaskExecutor;
    }

    /**
     * Handles a batch of messages received from Kafka.
     *
     * @param consumerRecords the records received from Kafka
     * @param acknowledgment  the acknowledgment object used to nack or ack the batch (partially)
     */
    @Override
    public void onMessage(List<ConsumerRecord<String, String>> consumerRecords, @NotNull Acknowledgment acknowledgment) {
        List<Future<PublishedMessageTaskResult>> taskFutureList = new ArrayList<>();
        int rejectedAtIndex = -1;

        // Submit tasks until rejection or all submitted
        for (int i = 0; i < consumerRecords.size(); i++) {
            var consumerRecord = consumerRecords.get(i);
            var task = getPublishedMessageTaskResultCallable(consumerRecord);
            try {
                Future<PublishedMessageTaskResult> taskFuture = taskExecutor.submit(task);
                taskFutureList.add(taskFuture);
            } catch (RejectedExecutionException e) {
                log.warn("Thread pool queue full, applying backpressure. Nacking batch from index {}", i);
                threadPoolSaturatedCounter.increment();
                rejectedAtIndex = i;
                break;
            }
        }

        // Wait for all submitted tasks to complete and check for failures
        var taskFailureIndex = -1;
        for (int index = 0; index < taskFutureList.size(); index++) {
            try {
                var taskResult = taskFutureList.get(index).get();
                if (!taskResult.isSuccessful() && taskFailureIndex == -1) {
                    taskFailureIndex = index;
                }
            } catch (ExecutionException | InterruptedException e) {
                log.error("Unexpected error processing event task at index {}", index, e);
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                taskFailureIndex = index;
                break;
            }
        }

        // Determine the earliest failure point (rejection or task failure)
        int finalNackIndex = determineNackIndex(rejectedAtIndex, taskFailureIndex);

        if (finalNackIndex < 0) {
            acknowledgment.acknowledge();
        } else {
            // Track nack metrics
            nackCounter.increment();
            if (rejectedAtIndex >= 0) {
                nackDueToRejectionCounter.increment();
            }
            if (taskFailureIndex >= 0) {
                nackDueToTaskFailureCounter.increment();
            }
            acknowledgment.nack(finalNackIndex, Duration.ofMillis(galaxyConfig.getNackSleepDurationMs()));
        }

    }

    /**
     * Determines the final nack index by taking the minimum of rejection and task failure indices.
     * This ensures we nack from the earliest failure point, whether it's due to thread pool saturation
     * or task execution failure.
     *
     * @param rejectedAtIndex  Index where thread pool rejected task submission (-1 if no rejection)
     * @param taskFailureIndex Index where a submitted task failed (-1 if no task failure)
     * @return The minimum valid index, or -1 if both are -1 (success)
     */
    private int determineNackIndex(int rejectedAtIndex, int taskFailureIndex) {
        if (rejectedAtIndex >= 0 && taskFailureIndex >= 0) {
            // Both failures occurred - use the earlier one
            return Math.min(rejectedAtIndex, taskFailureIndex);
        } else if (rejectedAtIndex >= 0) {
            // Only rejection occurred
            return rejectedAtIndex;
        } else {
            // Only task failure occurred (or no failure at all)
            return taskFailureIndex;
        }
    }

    /**
     * Returns a Callable task wrapped with the current trace context obtained from a consumer record.
     *
     * @param consumerRecord the consumer record used to create a task
     * @return a Callable task for processing the received message
     */
    @SuppressWarnings("unchecked")
    private Callable<PublishedMessageTaskResult> getPublishedMessageTaskResultCallable(ConsumerRecord<String, String> consumerRecord) {
        return (Callable<PublishedMessageTaskResult>) tracer.withCurrentTraceContext(publishedMessageTaskFactory.newTask(consumerRecord));
    }

}