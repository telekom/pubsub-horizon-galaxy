// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Test controller for development and testing purposes only.
 * This controller is only available when the 'dev' or 'test' profile is active.
 *
 * DO NOT enable in production!
 */
@RestController
@RequestMapping("/test")
@Profile({"dev", "test"})
@Slf4j
public class TestController {

    /**
     * Lists all running threads. Useful for debugging to find the correct Kafka thread names.
     */
    @GetMapping("/threads")
    public String listThreads() {
        return Thread.getAllStackTraces().keySet().stream()
            .map(Thread::getName)
            .sorted()
            .collect(Collectors.joining("\n"));
    }

    /**
     * Lists only Kafka-related threads.
     */
    @GetMapping("/kafka-threads")
    public String listKafkaThreads() {
        List<String> kafkaThreads = Thread.getAllStackTraces().keySet().stream()
            .map(Thread::getName)
            .filter(name -> name.matches("Container-\\d+-C-\\d+") ||  // Spring Kafka pattern
                           name.toLowerCase().contains("kafka") ||
                           name.contains("consumer") ||
                           name.contains("listener"))
            .sorted()
            .collect(Collectors.toList());

        if (kafkaThreads.isEmpty()) {
            return "No Kafka-related threads found.\n\nAll threads:\n" + listThreads();
        }
        return "Kafka threads (" + kafkaThreads.size() + "):\n" + String.join("\n", kafkaThreads);
    }

    /**
     * Interrupts ONE Kafka message listener thread to simulate an InterruptException.
     * This is useful for testing the fatal exception handling behavior.
     *
     * Note: Only one thread needs to be interrupted to trigger application shutdown,
     * since the error handler uses SpringApplication.exit() which terminates the entire JVM.
     *
     * Expected result: Application logs "Fatal Kafka consumer exception occurred" and shuts down.
     *
     * @return Information about the interrupted thread
     */
    @GetMapping("/interrupt-kafka")
    public String interruptKafka() {
        // Search for Spring Kafka container thread patterns:
        // - "Container-X-C-X" (default Spring Kafka naming)
        // - "kafka-message-listener" (custom bean name prefix)
        // - "ListenerContainer" (Spring Kafka container threads)
        Thread targetThread = Thread.getAllStackTraces().keySet().stream()
            .filter(t -> {
                String name = t.getName();
                return name.matches("Container-\\d+-C-\\d+") ||  // Spring Kafka default pattern
                       name.contains("kafka-message-listener") ||
                       name.contains("ListenerContainer");
            })
            .findFirst()
            .orElse(null);

        if (targetThread == null) {
            // Show helpful debug info
            List<String> allThreads = Thread.getAllStackTraces().keySet().stream()
                .map(Thread::getName)
                .filter(name -> name.contains("Container") ||
                               name.toLowerCase().contains("kafka") ||
                               name.contains("consumer"))
                .sorted()
                .collect(Collectors.toList());

            String debugInfo = allThreads.isEmpty()
                ? "No Kafka-related threads found at all."
                : "Found threads: " + String.join(", ", allThreads);

            log.warn("TEST: {}", debugInfo);
            return "No Kafka listener threads found to interrupt.\n\n" + debugInfo +
                   "\n\nTry: GET /test/kafka-threads to see all Kafka threads";
        }

        log.warn("TEST: Interrupting Kafka thread: {}", targetThread.getName());
        targetThread.interrupt();

        return "Interrupted thread: " + targetThread.getName() +
               "\n\nExpected: Application will shut down with exit code 1" +
               "\nCheck application logs for: 'Fatal Kafka consumer exception occurred'";
    }
}
