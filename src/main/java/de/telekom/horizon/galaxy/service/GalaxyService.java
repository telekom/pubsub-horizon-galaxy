// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.service;

import de.telekom.horizon.galaxy.StopGalaxyServiceEvent;
import de.telekom.horizon.galaxy.config.GalaxyConfig;
import jakarta.annotation.PostConstruct;
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
     * Handles the requested termination of Horizon Galaxy service
     */
    @EventListener
    public void onStopService(StopGalaxyServiceEvent event) {
        if (messageListenerContainer != null && messageListenerContainer.isRunning()) {
            log.warn("Stopping MessageListenerContainer. Reason: {}", event.getMessage());
            messageListenerContainer.stop();
        }
    }

    /**
     * Will be invoked when the message listener container stopped.
     * If the application is not shutting down already, exit immediately.
     */
    @EventListener(value = {ContainerStoppedEvent.class})
    public void containerStoppedHandler() {
        if (!context.isClosed()) {
            log.error("MessageListenerContainer stopped unexpectedly. Exiting in {} seconds...", config.getPreStopWaitTimeInSeconds());
            // wait for some time for ongoing message processing to finish
            try {
                Thread.sleep(Instant.ofEpochSecond(config.getPreStopWaitTimeInSeconds()).toEpochMilli());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            System.exit(SpringApplication.exit(context, () -> 1));
        }
    }
}
