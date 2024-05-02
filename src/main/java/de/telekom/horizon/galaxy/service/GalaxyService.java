// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.service;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.event.ContainerStoppedEvent;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class GalaxyService {

    private final ConcurrentMessageListenerContainer<String, String> messageListenerContainer;

    private final ApplicationContext context;

    public GalaxyService(ConcurrentMessageListenerContainer<String, String> messageListenerContainer,
                         ApplicationContext context) {
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

    @EventListener
    public void containerStoppedHandler(ContainerStoppedEvent event) {
        log.error("MessageListenerContainer stopped with event {}. Exiting...", event.toString());
        if (messageListenerContainer != null) {
            messageListenerContainer.stop();
        }
        SpringApplication.exit(context, () -> -2);
    }
}
