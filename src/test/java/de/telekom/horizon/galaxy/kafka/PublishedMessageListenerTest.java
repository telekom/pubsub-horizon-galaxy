// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.kafka;

import de.telekom.eni.pandora.horizon.tracing.HorizonTracer;
import de.telekom.horizon.galaxy.config.GalaxyConfig;
import de.telekom.horizon.galaxy.model.PublishedMessageTaskResult;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class PublishedMessageListenerTest {

    private PublishedMessageTaskFactory factory;
    private HorizonTracer tracer;
    private GalaxyConfig galaxyConfig;
    private SimpleMeterRegistry meterRegistry;
    private Acknowledgment acknowledgment;
    private PublishedMessageListener listener;

    @BeforeEach
    void setUp() {
        factory = mock(PublishedMessageTaskFactory.class);
        tracer = mock(HorizonTracer.class);
        galaxyConfig = new GalaxyConfig();
        galaxyConfig.setBatchCoreThreadPoolSize(2);
        galaxyConfig.setBatchMaxThreadPoolSize(2);
        galaxyConfig.setBatchQueueCapacity(5);
        galaxyConfig.setNackSleepDurationMs(1000);
        meterRegistry = new SimpleMeterRegistry();
        acknowledgment = mock(Acknowledgment.class);

        // Make tracer.withCurrentTraceContext pass through the callable
        when(tracer.withCurrentTraceContext(any(Callable.class))).thenAnswer(invocation -> invocation.getArgument(0));

        listener = new PublishedMessageListener(factory, tracer, galaxyConfig, meterRegistry);
    }

    @Test
    void onMessageShouldAcknowledgeWhenAllTasksSucceed() {
        var record = new ConsumerRecord<>("topic", 0, 0L, "key", "value");
        var task = mock(PublishedMessageTask.class);
        when(factory.newTask(any())).thenReturn(task);
        when(task.call()).thenReturn(new PublishedMessageTaskResult(true));

        listener.onMessage(List.of(record), acknowledgment);

        verify(acknowledgment).acknowledge();
        verify(acknowledgment, never()).nack(anyInt(), any(Duration.class));
    }

    @Test
    void onMessageShouldNackOnTaskFailure() {
        var record = new ConsumerRecord<>("topic", 0, 0L, "key", "value");
        var task = mock(PublishedMessageTask.class);
        when(factory.newTask(any())).thenReturn(task);
        when(task.call()).thenReturn(new PublishedMessageTaskResult(false));

        listener.onMessage(List.of(record), acknowledgment);

        verify(acknowledgment, never()).acknowledge();
        verify(acknowledgment).nack(eq(0), any(Duration.class));
    }

    @Test
    void onMessageShouldNackOnRejection() {
        // Fill the thread pool to force rejection
        galaxyConfig.setBatchCoreThreadPoolSize(1);
        galaxyConfig.setBatchMaxThreadPoolSize(1);
        galaxyConfig.setBatchQueueCapacity(0);
        var smallPoolListener = new PublishedMessageListener(factory, tracer, galaxyConfig, meterRegistry);

        // Create a blocking task that occupies the single thread
        var blockingTask = mock(PublishedMessageTask.class);
        when(factory.newTask(any())).thenReturn(blockingTask);
        when(blockingTask.call()).thenAnswer(invocation -> {
            Thread.sleep(2000);
            return new PublishedMessageTaskResult(true);
        });

        var record1 = new ConsumerRecord<>("topic", 0, 0L, "key1", "value1");
        var record2 = new ConsumerRecord<>("topic", 0, 1L, "key2", "value2");
        var record3 = new ConsumerRecord<>("topic", 0, 2L, "key3", "value3");

        smallPoolListener.onMessage(List.of(record1, record2, record3), acknowledgment);

        verify(acknowledgment, never()).acknowledge();
        verify(acknowledgment).nack(anyInt(), any(Duration.class));
    }

    @Test
    void onMessageShouldNackAtEarliestFailureWhenBothRejectionAndTaskFailure() {
        // Use a pool that can accept exactly 1 task (queue=0, 1 thread)
        galaxyConfig.setBatchCoreThreadPoolSize(1);
        galaxyConfig.setBatchMaxThreadPoolSize(1);
        galaxyConfig.setBatchQueueCapacity(0);
        var smallPoolListener = new PublishedMessageListener(factory, tracer, galaxyConfig, meterRegistry);

        // First task fails, second gets rejected
        var failingTask = mock(PublishedMessageTask.class);
        when(factory.newTask(any())).thenReturn(failingTask);
        when(failingTask.call()).thenAnswer(invocation -> {
            Thread.sleep(500);
            return new PublishedMessageTaskResult(false);
        });

        var record1 = new ConsumerRecord<>("topic", 0, 0L, "key1", "value1");
        var record2 = new ConsumerRecord<>("topic", 0, 1L, "key2", "value2");

        smallPoolListener.onMessage(List.of(record1, record2), acknowledgment);

        // Should nack at index 0 (task failure at 0 is earlier than rejection at 1)
        verify(acknowledgment).nack(eq(0), any(Duration.class));
    }

    @Test
    void onMessageShouldIncrementNackCounterOnFailure() {
        var record = new ConsumerRecord<>("topic", 0, 0L, "key", "value");
        var task = mock(PublishedMessageTask.class);
        when(factory.newTask(any())).thenReturn(task);
        when(task.call()).thenReturn(new PublishedMessageTaskResult(false));

        listener.onMessage(List.of(record), acknowledgment);

        assertEquals(1.0, meterRegistry.get("pubsub.kafka.listener.nacks").counter().count());
        assertEquals(1.0, meterRegistry.get("pubsub.kafka.listener.nacks.task_failure").counter().count());
    }

    @Test
    void onMessageShouldAcknowledgeEmptyBatch() {
        listener.onMessage(List.of(), acknowledgment);

        verify(acknowledgment).acknowledge();
    }
}
