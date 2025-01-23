// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.service;

import de.telekom.horizon.galaxy.StopMessageListenerEvent;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.event.ContainerStoppedEvent;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class GalaxyService {

    private final ConcurrentMessageListenerContainer<String, String> messageListenerContainer;

    private final ConfigurableApplicationContext context;

    public GalaxyService(ConcurrentMessageListenerContainer<String, String> messageListenerContainer,
                         ConfigurableApplicationContext context) {
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
     * Handles the requested termination of the message listener container
     */
    @EventListener(value = {StopMessageListenerEvent.class})
    public void onStopMessageListenerEvent() {
        if (messageListenerContainer != null && messageListenerContainer.isRunning()) {
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
            log.error("MessageListenerContainer stopped unexpectedly. Exiting now...");
            System.exit(SpringApplication.exit(context, () -> 1));
        } else {
            log.warn("MessageListenerContainer stopped due to application shutdown event.");
        }
    }
}
