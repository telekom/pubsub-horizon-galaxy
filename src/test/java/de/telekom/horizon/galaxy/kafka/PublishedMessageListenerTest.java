// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.kafka;

import brave.Span;
import de.telekom.eni.pandora.horizon.tracing.HorizonTracer;
import de.telekom.horizon.galaxy.config.GalaxyConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private Executor executor;
    private int kafkaMaxPollIntervalMs;

    @BeforeEach
    void setUp() {
        factory = mock(PublishedMessageTaskFactory.class);
        tracer = mock(HorizonTracer.class);
        galaxyConfig = new GalaxyConfig();
        galaxyConfig.setNackSleepDurationMs(1000);
        meterRegistry = new SimpleMeterRegistry();
        acknowledgment = mock(Acknowledgment.class);
        Span span = mock(Span.class);
        executor = mock(ExecutorService.class);
        kafkaMaxPollIntervalMs = 300_000;

        // Make tracer.withCurrentTraceContext pass through the callable
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(executor).execute(any());
        when(tracer.<CompletableFuture<Void>>withCurrentContext(any())).thenAnswer(invocation -> invocation.getArgument(0));
        when(tracer.startSpanFromKafkaHeaders(any(), any())).thenReturn(span);

        listener = new PublishedMessageListener(factory, tracer, galaxyConfig, meterRegistry, executor, kafkaMaxPollIntervalMs);
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
        var smallPoolListener = new PublishedMessageListener(factory, tracer, galaxyConfig, meterRegistry, executor, kafkaMaxPollIntervalMs);

        var record1 = new ConsumerRecord<>("topic", 0, 0L, "key1", "value1");
        var record2 = new ConsumerRecord<>("topic", 0, 1L, "key2", "value2");

        // First task fails, second gets rejected
        var failingTask = mock(PublishedMessageTask.class);
        var normalButRejectedTask = mock(PublishedMessageTask.class);
        when(factory.newTask(record1)).thenReturn(failingTask);
        when(factory.newTask(record2)).thenReturn(normalButRejectedTask);
        when(failingTask.call()).thenReturn(CompletableFuture.failedFuture(new Exception("onMessageShouldNackAtEarliestFailure test exception")));
        when(normalButRejectedTask.call()).thenReturn(CompletableFuture.completedFuture(null));

        final var counter = new int[]{0};
        reset(executor);
        doAnswer(invocation -> {
            if (counter[0] == 0) {
                counter[0]++;
                invocation.<Runnable>getArgument(0).run();
            } else {
                throw new RejectedExecutionException("Executor Mock: only first task is executed, any other is rejected");
            }
            return null;
        }).when(executor).execute(any(Runnable.class));

        // Should nack at index 0 (task failure at 0 is earlier than rejection at 1)
        assertThrows(RejectedExecutionException.class, () -> smallPoolListener.onMessage(List.of(record1, record2), acknowledgment));
        verify(failingTask).call();
        verify(normalButRejectedTask, never()).call();
        verify(executor, times(2)).execute(any(Runnable.class));
        verify(factory, times(2)).newTask(any());
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

    @Nested
    class TaskSubmissionLoopTest {

        // Empty Batch (consumerRecords.isEmpty())
        @Test
        void onMessageShouldSkipSubmissionLoopWhenBatchIsEmpty() {
            listener.onMessage(List.of(), acknowledgment);

            verifyNoInteractions(factory, tracer);
            verify(acknowledgment).acknowledge();
            verify(acknowledgment, never()).nack(anyInt(), any(Duration.class));
        }

        // All Tasks Submitted Successfully
        @Test
        void onMessageShouldSubmitAllTasksWhenPoolHasCapacity() {
            var record0 = new ConsumerRecord<>("topic", 0, 0L, "key0", "value0");
            var record1 = new ConsumerRecord<>("topic", 0, 1L, "key1", "value1");
            var task0 = mock(PublishedMessageTask.class);
            var task1 = mock(PublishedMessageTask.class);

            when(factory.newTask(record0)).thenReturn(task0);
            when(factory.newTask(record1)).thenReturn(task1);
            when(task0.call()).thenReturn(CompletableFuture.completedFuture(null));
            when(task1.call()).thenReturn(CompletableFuture.completedFuture(null));

            listener.onMessage(List.of(record0, record1), acknowledgment);

            verify(factory).newTask(record0);
            verify(factory).newTask(record1);
            verify(task0).call();
            verify(task1).call();
            verify(acknowledgment).acknowledge();
            verify(acknowledgment, never()).nack(anyInt(), any(Duration.class));
        }

        // Thread Pool Saturation (RejectedExecutionException at index i, breaks loop)
        // NB! The case as of now is purely theoretical, since current Executor is unbounded
        @Test
        void onMessageShouldBreakSubmissionLoopAndNackAtRejectedIndexWhenPoolSaturated() {
            var record0 = new ConsumerRecord<>("topic", 0, 0L, "key0", "value0");
            var record1 = new ConsumerRecord<>("topic", 0, 1L, "key1", "value1");
            var record2 = new ConsumerRecord<>("topic", 0, 2L, "key2", "value2");
            var blockingTask = mock(PublishedMessageTask.class);
            var rejectedTask = mock(PublishedMessageTask.class);

            when(factory.newTask(record0)).thenReturn(blockingTask);
            when(factory.newTask(record1)).thenReturn(rejectedTask);
            when(blockingTask.call()).thenAnswer(invocation -> {
                Thread.sleep(300);
                return CompletableFuture.completedFuture(null);
            });

            try (var executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>())) {
                var saturatedListener = new PublishedMessageListener(factory, tracer, galaxyConfig, meterRegistry, executor, kafkaMaxPollIntervalMs);
                assertThrows(RejectedExecutionException.class, () -> saturatedListener.onMessage(List.of(record0, record1, record2), acknowledgment));
            }

            verify(factory).newTask(record0);
            verify(factory).newTask(record1);
            verify(factory, never()).newTask(record2);
            verify(rejectedTask, never()).call();
            verify(acknowledgment, never()).acknowledge();
            verify(acknowledgment, never()).nack(anyInt(), any());
        }
    }

    @Nested
    class TaskCompletionAndFailureDetectionLoopA2Test {

        // All tasks return isSuccessful() == true
        @Test
        void onMessageShouldContinueAndAcknowledgeWhenAllTasksSucceed() {
            var record0 = new ConsumerRecord<>("topic", 0, 0L, "key0", "value0");
            var record1 = new ConsumerRecord<>("topic", 0, 1L, "key1", "value1");
            var task0 = mock(PublishedMessageTask.class);
            var task1 = mock(PublishedMessageTask.class);

            when(factory.newTask(record0)).thenReturn(task0);
            when(factory.newTask(record1)).thenReturn(task1);
            when(task0.call()).thenReturn(CompletableFuture.completedFuture(null));
            when(task1.call()).thenReturn(CompletableFuture.completedFuture(null));

            listener.onMessage(List.of(record0, record1), acknowledgment);

            verify(task0).call();
            verify(task1).call();
            verify(acknowledgment).acknowledge();
            verify(acknowledgment, never()).nack(anyInt(), any(Duration.class));
        }

        // Task returns isSuccessful() == false (first + subsequent failure, waits for remaining tasks)
        @Test
        void onMessageShouldRecordFirstFailureIndexAndWaitForAllRemainingTasks() {
            var record0 = new ConsumerRecord<>("topic", 0, 0L, "key0", "value0");
            var record1 = new ConsumerRecord<>("topic", 0, 1L, "key1", "value1");
            var record2 = new ConsumerRecord<>("topic", 0, 2L, "key2", "value2");
            var task0 = mock(PublishedMessageTask.class);
            var task1 = mock(PublishedMessageTask.class);
            var task2 = mock(PublishedMessageTask.class);
            var task2Completed = new AtomicBoolean(false);

            when(factory.newTask(record0)).thenReturn(task0);
            when(factory.newTask(record1)).thenReturn(task1);
            when(factory.newTask(record2)).thenReturn(task2);

            when(task0.call()).thenReturn(CompletableFuture.completedFuture(null));
            when(task1.call()).thenReturn(CompletableFuture.failedFuture(new Exception("task 1: test exception"))); // first failure at index 1
            when(task2.call()).thenAnswer(invocation -> {
                Thread.sleep(100);
                task2Completed.set(true);
                return CompletableFuture.failedFuture(new Exception("task 2: test exception")); // subsequent failure at index 2
            });

            listener.onMessage(List.of(record0, record1, record2), acknowledgment);

            assertTrue(task2Completed.get(), "Loop must not break on Result(false) and must wait for remaining tasks");
            verify(acknowledgment, never()).acknowledge();
            verify(acknowledgment).nack(eq(1), eq(Duration.ofMillis(1000)));
        }

        // Future.get() throws ExecutionException
        @Test
        void onMessageShouldWaitForAllAndNackWhenTaskThrowsExecutionException() {
            var record0 = new ConsumerRecord<>("topic", 0, 0L, "key0", "value0");
            var record1 = new ConsumerRecord<>("topic", 0, 1L, "key1", "value1");
            var task0 = mock(PublishedMessageTask.class);
            var task1 = mock(PublishedMessageTask.class);

            when(factory.newTask(record0)).thenReturn(task0);
            when(factory.newTask(record1)).thenReturn(task1);
            when(task0.call()).thenThrow(new RuntimeException("Uncaught task failure"));
            when(task1.call()).thenAnswer(invocation -> {
                Thread.sleep(100);
                return CompletableFuture.completedFuture(null);
            });

                listener.onMessage(List.of(record0, record1), acknowledgment);

                assertFalse(Thread.currentThread().isInterrupted());
                verify(acknowledgment, never()).acknowledge();
                verify(acknowledgment).nack(eq(0), eq(Duration.ofMillis(1000)));

        }

        // Future.get() throws ExecutionException after an earlier failure (retains earlier taskFailureIndex)
        @Test
        void onMessageShouldRetainEarlierFailureIndexWhenSubsequentTaskThrowsExecutionException() {
            var record0 = new ConsumerRecord<>("topic", 0, 0L, "key0", "value0");
            var record1 = new ConsumerRecord<>("topic", 0, 1L, "key1", "value1");
            var task0 = mock(PublishedMessageTask.class);
            var task1 = mock(PublishedMessageTask.class);

            when(factory.newTask(record0)).thenReturn(task0);
            when(factory.newTask(record1)).thenReturn(task1);
            when(task0.call()).thenReturn(CompletableFuture.failedFuture(new Exception("test"))); // failure at index 0
            when(task1.call()).thenThrow(new RuntimeException("Uncaught failure at index 1"));

            listener.onMessage(List.of(record0, record1), acknowledgment);

            verify(acknowledgment, never()).acknowledge();
            verify(acknowledgment).nack(eq(0), eq(Duration.ofMillis(1000)));
        }

        // Future.get() throws InterruptedException (re-interrupts thread and breaks)
        @Test
        void onMessageShouldRestoreInterruptFlagAndBreakWhenInterruptedWhileWaiting() throws InterruptedException {
            try (var singleThreadExecutor = Executors.newSingleThreadExecutor()) {
                reset(executor);
                doAnswer(invocation -> {
                    singleThreadExecutor.execute(invocation.getArgument(0));
                    return null;
                }).when(executor).execute(any());
                var record0 = new ConsumerRecord<>("topic", 0, 0L, "key0", "value0");
                var task0 = mock(PublishedMessageTask.class);
                var releaseTask0 = new CountDownLatch(1);
                var interruptLatch = new CountDownLatch(1);

                when(factory.newTask(record0)).thenReturn(task0);
                when(task0.call()).thenAnswer(invocation -> {
                    interruptLatch.countDown();
                    releaseTask0.await();
                    return CompletableFuture.completedFuture(null);
                });

                try {
                    var thread = new Thread(() -> listener.onMessage(List.of(record0), acknowledgment));
                    thread.start();

                    interruptLatch.await();
                    thread.interrupt();
                    thread.join();

                    assertTrue(thread.isInterrupted(), "Listener must restore interrupt status");
                    verify(acknowledgment, never()).acknowledge();
                    verify(acknowledgment).nack(eq(0), eq(Duration.ofMillis(1000)));
                } finally {
                    releaseTask0.countDown();
                }
            }
        }
    }

    @Nested
    class NackIndexResolutionAndAcknowledgmentTest {

        @Test
        void onMessageShouldAcknowledgeBatchAndNotIncrementNackCountersWhenNoRejectionOrTaskFailure() {
            var record = new ConsumerRecord<>("topic", 0, 0L, "key", "value");
            var task = mock(PublishedMessageTask.class);
            when(factory.newTask(record)).thenReturn(task);
            when(task.call()).thenReturn(CompletableFuture.completedFuture(null));

            listener.onMessage(List.of(record), acknowledgment);

            verify(acknowledgment).acknowledge();
            verify(acknowledgment, never()).nack(anyInt(), any(Duration.class));
            assertEquals(0.0, meterRegistry.get("pubsub.kafka.listener.nacks").counter().count());
            assertEquals(0.0, meterRegistry.get("pubsub.kafka.listener.nacks.task_failure").counter().count());
        }

        @Test
        void onMessageShouldThrowWhenOnlyRejectionOccurs() {
            final var maxPollInterval = 3_000;
            try (var singleThreadExecutor = new ThreadPoolExecutor(1, 1, 10_000, TimeUnit.MILLISECONDS, new SynchronousQueue<>())) {
                var smallPoolListener = new PublishedMessageListener(factory, tracer, galaxyConfig, meterRegistry, singleThreadExecutor, maxPollInterval);

                var record0 = new ConsumerRecord<>("topic", 0, 0L, "key0", "value0");
                var record1 = new ConsumerRecord<>("topic", 0, 1L, "key1", "value1");
                var task0 = mock(PublishedMessageTask.class);
                var task1 = mock(PublishedMessageTask.class);

                when(factory.newTask(record0)).thenReturn(task0);
                when(factory.newTask(record1)).thenReturn(task1);
                when(task0.call()).thenAnswer(invocation -> {
                    Thread.sleep(200);
                    return CompletableFuture.completedFuture(null);
                });

                assertThrows(RejectedExecutionException.class, () -> smallPoolListener.onMessage(List.of(record0, record1), acknowledgment));

                verify(acknowledgment, never()).acknowledge();
                verify(acknowledgment, never()).nack(anyInt(), any(Duration.class));
                assertEquals(0.0, meterRegistry.get("pubsub.kafka.listener.nacks").counter().count());
                assertEquals(0.0, meterRegistry.get("pubsub.kafka.listener.nacks.task_failure").counter().count());
            }
        }

        // Task failure without rejection
        @Test
        void onMessageShouldNackAtTaskFailureIndexAndIncrementBothCountersWhenTaskFailsWithoutRejection() {
            var record0 = new ConsumerRecord<>("topic", 0, 0L, "key0", "value0");
            var record1 = new ConsumerRecord<>("topic", 0, 1L, "key1", "value1");
            var task0 = mock(PublishedMessageTask.class);
            var task1 = mock(PublishedMessageTask.class);

            when(factory.newTask(record0)).thenReturn(task0);
            when(factory.newTask(record1)).thenReturn(task1);
            when(task0.call()).thenReturn(CompletableFuture.completedFuture(null));
            when(task1.call()).thenReturn(CompletableFuture.failedFuture(new Exception("test")));

            listener.onMessage(List.of(record0, record1), acknowledgment);

            verify(acknowledgment, never()).acknowledge();
            verify(acknowledgment).nack(eq(1), eq(Duration.ofMillis(galaxyConfig.getNackSleepDurationMs())));
            assertEquals(1.0, meterRegistry.get("pubsub.kafka.listener.nacks").counter().count());
            assertEquals(1.0, meterRegistry.get("pubsub.kafka.listener.nacks.task_failure").counter().count());
        }

        // Task failure with subsequent rejection
        @Test
        void onMessageShouldThrowWhenBothTaskFailureAndRejectionOccur() {
            final var maxPollInterval = 3_000;
            try (var singleThreadExecutor = new ThreadPoolExecutor(1, 1, 10_000, TimeUnit.MILLISECONDS, new SynchronousQueue<>())) {
                var smallPoolListener = new PublishedMessageListener(factory, tracer, galaxyConfig, meterRegistry, singleThreadExecutor, maxPollInterval);

                var record0 = new ConsumerRecord<>("topic", 0, 0L, "key0", "value0");
                var record1 = new ConsumerRecord<>("topic", 0, 1L, "key1", "value1");
                var task0 = mock(PublishedMessageTask.class);
                var task1 = mock(PublishedMessageTask.class);

                when(factory.newTask(record0)).thenReturn(task0);
                when(factory.newTask(record1)).thenReturn(task1);
                when(task0.call()).thenAnswer(invocation -> {
                    Thread.sleep(200);
                    return CompletableFuture.failedFuture(new Exception("test"));
                });

                assertThrows(RejectedExecutionException.class, () -> smallPoolListener.onMessage(List.of(record0, record1), acknowledgment));
                verify(acknowledgment, never()).acknowledge();
                verify(acknowledgment, never()).nack(anyInt(), any(Duration.class));
                assertEquals(0.0, meterRegistry.get("pubsub.kafka.listener.nacks").counter().count());
                assertEquals(0.0, meterRegistry.get("pubsub.kafka.listener.nacks.task_failure").counter().count());
            }
        }
    }
}
