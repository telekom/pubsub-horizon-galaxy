// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.eni.pandora.horizon.model.event.SubscriptionEventMessage;
import de.telekom.horizon.galaxy.kafka.PublishedMessageTaskFactory;
import de.telekom.horizon.galaxy.utils.AbstractIntegrationTest;
import de.telekom.horizon.galaxy.utils.HazelcastTestInstance;
import de.telekom.horizon.galaxy.utils.HorizonTestHelper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(HazelcastTestInstance.class)
class ThreadPoolMetricsTest extends AbstractIntegrationTest {

    private static final String TEST_ENVIRONMENT = "playground";

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private PublishedMessageTaskFactory publishedMessageTaskFactory;

    @Test
    void multiplexMetricsAreRegistered() {
        // Counters
        Counter poolsCreated = meterRegistry.find("galaxy.multiplex.pools.created").counter();
        Counter tasksCompleted = meterRegistry.find("galaxy.multiplex.tasks.completed").counter();
        assertNotNull(poolsCreated, "galaxy.multiplex.pools.created counter should be registered");
        assertNotNull(tasksCompleted, "galaxy.multiplex.tasks.completed counter should be registered");

        // Config gauges
        Gauge coreSize = meterRegistry.find("galaxy.multiplex.pool.core.size").gauge();
        Gauge maxSize = meterRegistry.find("galaxy.multiplex.pool.max.size").gauge();
        assertNotNull(coreSize, "galaxy.multiplex.pool.core.size gauge should be registered");
        assertNotNull(maxSize, "galaxy.multiplex.pool.max.size gauge should be registered");
        assertEquals(10.0, coreSize.value(), "core pool size should match configured value");
        assertEquals(20.0, maxSize.value(), "max pool size should match configured value");

        // State gauges
        Gauge activeThreads = meterRegistry.find("galaxy.multiplex.active.threads").gauge();
        Gauge poolCount = meterRegistry.find("galaxy.multiplex.pool.count").gauge();
        assertNotNull(activeThreads, "galaxy.multiplex.active.threads gauge should be registered");
        assertNotNull(poolCount, "galaxy.multiplex.pool.count gauge should be registered");
    }

    @Test
    void batchTaskExecutorMetricsAreRegistered() {
        Gauge corePoolSize = meterRegistry.find("executor.pool.core").tag("name", "batchTaskExecutor").gauge();
        Gauge maxPoolSize = meterRegistry.find("executor.pool.max").tag("name", "batchTaskExecutor").gauge();

        assertNotNull(corePoolSize, "executor.pool.core metric should be registered for batchTaskExecutor");
        assertNotNull(maxPoolSize, "executor.pool.max metric should be registered for batchTaskExecutor");
        assertEquals(5.0, corePoolSize.value(), "batch core pool size should match configured value");
        assertEquals(100.0, maxPoolSize.value(), "batch max pool size should match configured value");
    }

    @Test
    void gaugesReflectAtomicIntegerValues() {
        // Directly manipulate the AtomicIntegers and verify the gauges read the correct values.
        // This proves the gauge wiring works — the gauge reads from the AtomicInteger.
        var activeThreads = publishedMessageTaskFactory.getMultiplexActiveThreads();
        var poolCount = publishedMessageTaskFactory.getMultiplexPoolCount();

        Gauge activeGauge = meterRegistry.find("galaxy.multiplex.active.threads").gauge();
        Gauge poolGauge = meterRegistry.find("galaxy.multiplex.pool.count").gauge();
        assertNotNull(activeGauge);
        assertNotNull(poolGauge);

        // Set known values
        int prevActive = activeThreads.get();
        int prevPool = poolCount.get();

        activeThreads.set(7);
        poolCount.set(3);

        assertEquals(7.0, activeGauge.value(), "active threads gauge should reflect AtomicInteger value");
        assertEquals(3.0, poolGauge.value(), "pool count gauge should reflect AtomicInteger value");

        // Restore
        activeThreads.set(prevActive);
        poolCount.set(prevPool);
    }

    @Test
    void gaugesIncrementDuringLoadAndReturnToZeroAfter() throws Exception {
        // Set up 3 subscriptions per event — each event creates 1 pool with 3 async send tasks
        List<SubscriptionResource> subscriptions = List.of(
                HorizonTestHelper.createDefaultSubscriptionResourceWithSubscriberId(TEST_ENVIRONMENT, getEventType(), "sub-1"),
                HorizonTestHelper.createDefaultSubscriptionResourceWithSubscriberId(TEST_ENVIRONMENT, getEventType(), "sub-2"),
                HorizonTestHelper.createDefaultSubscriptionResourceWithSubscriberId(TEST_ENVIRONMENT, getEventType(), "sub-3")
        );
        when(subscriptionCacheMock.getSubscriptionsForEnvironmentAndEventType(TEST_ENVIRONMENT, getEventType()))
                .thenReturn(subscriptions);

        // Track the max observed gauge values from a polling thread
        AtomicInteger maxPoolCount = new AtomicInteger(0);
        AtomicInteger maxActiveThreads = new AtomicInteger(0);
        AtomicBoolean polling = new AtomicBoolean(true);

        Thread poller = new Thread(() -> {
            while (polling.get()) {
                int pc = publishedMessageTaskFactory.getMultiplexPoolCount().get();
                int at = publishedMessageTaskFactory.getMultiplexActiveThreads().get();
                maxPoolCount.updateAndGet(prev -> Math.max(prev, pc));
                maxActiveThreads.updateAndGet(prev -> Math.max(prev, at));
            }
        });
        poller.start();

        double poolsCreatedBefore = publishedMessageTaskFactory.getMultiplexPoolsCreated().count();
        double tasksCompletedBefore = publishedMessageTaskFactory.getMultiplexTasksCompleted().count();

        // Send 20 events rapidly — creates 20 concurrent pools with 60 total async tasks.
        // The batch pool (core=5) processes up to 5 events concurrently, so multiple
        // multiplex pools will be alive at the same time.
        int eventCount = 20;
        for (int i = 0; i < eventCount; i++) {
            simulateNewPublishedEvent(getPublishedEventMessage("{\"test\": \"data-" + i + "\"}"));
        }

        // Wait for all outbound events (20 events * 3 subscriptions = 60)
        List<SubscriptionEventMessage> received = receiveOutboundEvents();

        // Stop polling
        polling.set(false);
        poller.join(5000);

        // Verify we received all expected events
        assertEquals(eventCount * 3, received.size(),
                "Should receive " + (eventCount * 3) + " multiplexed events");

        // Counters must have incremented
        double poolsCreatedAfter = publishedMessageTaskFactory.getMultiplexPoolsCreated().count();
        double tasksCompletedAfter = publishedMessageTaskFactory.getMultiplexTasksCompleted().count();

        assertTrue(poolsCreatedAfter >= poolsCreatedBefore + eventCount,
                "pools.created should have incremented by at least " + eventCount
                        + ", was " + poolsCreatedBefore + " now " + poolsCreatedAfter);
        assertTrue(tasksCompletedAfter >= tasksCompletedBefore + (eventCount * 3),
                "tasks.completed should have incremented by at least " + (eventCount * 3)
                        + ", was " + tasksCompletedBefore + " now " + tasksCompletedAfter);

        // The poller must have observed non-zero values during processing
        assertTrue(maxPoolCount.get() > 0,
                "pool count gauge should have been > 0 during processing, max observed: " + maxPoolCount.get());
        assertTrue(maxActiveThreads.get() > 0,
                "active threads gauge should have been > 0 during processing, max observed: " + maxActiveThreads.get());

        // After all processing completes, gauges must return to 0
        assertEquals(0, publishedMessageTaskFactory.getMultiplexPoolCount().get(),
                "pool count should return to 0 after processing completes");
        assertEquals(0, publishedMessageTaskFactory.getMultiplexActiveThreads().get(),
                "active threads should return to 0 after processing completes");
    }
}
