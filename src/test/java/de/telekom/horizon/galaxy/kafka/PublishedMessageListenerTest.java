// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.kafka;

import de.telekom.eni.pandora.horizon.tracing.HorizonTracer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class PublishedMessageListenerTest {

    private PublishedMessageTaskFactory factory;
    private HorizonTracer tracer;
    private SimpleMeterRegistry meterRegistry;
    private Acknowledgment acknowledgment;
    private PublishedMessageListener listener;

    @BeforeEach
    void setUp() {
        factory = mock(PublishedMessageTaskFactory.class);
        tracer = mock(HorizonTracer.class);
        meterRegistry = new SimpleMeterRegistry();
        acknowledgment = mock(Acknowledgment.class);

        // Make tracer.withCurrentTraceContext pass through the callable
        when(tracer.<CompletableFuture<Void>>withCurrentContext(any())).thenAnswer(invocation -> invocation.getArgument(0));

        listener = new PublishedMessageListener(factory, tracer, meterRegistry);
    }

    @Test
    void onMessageShouldAcknowledgeWhenAllTasksSucceed() {
        var record = new ConsumerRecord<>("topic", 0, 0L, "key", "value");
        var task = mock(PublishedMessageTask.class);
        when(factory.newTask(any())).thenReturn(task);
        when(task.call()).thenReturn(CompletableFuture.completedFuture(null));

        listener.onMessage(List.of(record), acknowledgment);

        verify(acknowledgment).acknowledge();
        verify(acknowledgment, never()).nack(anyInt(), any(Duration.class));
    }

    @Test
    void onMessageShouldNackOnTaskFailure() {
        var record = new ConsumerRecord<>("topic", 0, 0L, "key", "value");
        var task = mock(PublishedMessageTask.class);
        when(factory.newTask(any())).thenReturn(task);
        when(task.call()).thenReturn(CompletableFuture.failedFuture(new Exception("onMessageShouldNackOnTaskFailure test exception")));

        listener.onMessage(List.of(record), acknowledgment);

        verify(acknowledgment, never()).acknowledge();
        verify(acknowledgment).nack(eq(0), any(Duration.class));
    }

    @Test
    void onMessageShouldNackAtEarliestFailureWhenBothRejectionAndTaskFailure() {
        var smallPoolListener = new PublishedMessageListener(factory, tracer, meterRegistry);

        // First task fails, second gets rejected
        var failingTask = mock(PublishedMessageTask.class);
        when(factory.newTask(any())).thenReturn(failingTask);
        when(failingTask.call()).thenAnswer(invocation -> {
            Thread.sleep(500);
            return CompletableFuture.failedFuture(new Exception("onMessageShouldNackAtEarliestFailure test exception"));
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
        when(task.call()).thenReturn(CompletableFuture.failedFuture(new Exception("onMessageShouldIncrementNackCounterOnFailure test exception")));

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
