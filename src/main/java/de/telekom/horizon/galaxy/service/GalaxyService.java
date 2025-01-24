// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.service;

import de.telekom.horizon.galaxy.config.GalaxyConfig;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.event.ContainerStoppedEvent;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
@Slf4j
public class GalaxyService {

    private final GalaxyConfig config;

    private final ConcurrentMessageListenerContainer<String, String> messageListenerContainer;

    private final ConfigurableApplicationContext context;

    public GalaxyService(GalaxyConfig config, ConcurrentMessageListenerContainer<String, String> messageListenerContainer,
                         ConfigurableApplicationContext context) {
        this.config = config;
        this.messageListenerContainer = messageListenerContainer;
        this.context = context;
    }

    @PostConstruct
    public void init() {
        if (messageListenerContainer != null) {
            messageListenerContainer.start();

            log.info("ConcurrentMessageListenerContainer started.");
        }
    }

    /**
     * Will be invoked when the bean should be destroyed due to an application shutdown
     *
     * Stops the message listener container and therefore the further processing of messages
     *
     */
    @PreDestroy
    public void stopService() {
        // stop message listener container
        if (messageListenerContainer != null && messageListenerContainer.isRunning()) {
            log.warn("Stopping MessageListenerContainer due to application shutdown.");
            messageListenerContainer.stop();
        }
    }

    /**
     * Will be invoked when the message listener container stopped
     *
     * Initiates the graceful shutdown of the application
     *
     */
    @EventListener(value = {ContainerStoppedEvent.class})
    public void containerStoppedHandler() {
        gracefulShutdown();
    }

    /**
     * Handles the graceful shutdown of the application
     *
     * The method checks whether the application's context already has been closed. Depending on the outcome
     * the shutdown will be handled either as expected or unexpected.
     *
     */
    private void gracefulShutdown() {
        var isContextClosed = context.isClosed();

        if (isContextClosed) {
            log.warn("MessageListenerContainer stopped. Exiting application in {} seconds...", config.getShutdownWaitTimeSeconds());
        } else {
            log.error("MessageListenerContainer stopped unexpectedly. Exiting application in {} seconds...", config.getShutdownWaitTimeSeconds());
        }

        // wait for some time for ongoing tasks to finish
        try {
            Thread.sleep(Instant.ofEpochSecond(config.getShutdownWaitTimeSeconds()).toEpochMilli());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.exit(SpringApplication.exit(context, () -> isContextClosed ? 0 : 1));
    }
}
