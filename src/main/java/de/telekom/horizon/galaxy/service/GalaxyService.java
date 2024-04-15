// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.service;

import de.telekom.eni.pandora.horizon.exception.CouldNotStartInformerException;
import de.telekom.eni.pandora.horizon.kubernetes.ListenerEvent;
import de.telekom.eni.pandora.horizon.kubernetes.SubscriptionResourceListener;
import de.telekom.horizon.galaxy.cache.SubscriptionCache;
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

    private final ApplicationContext applicationContext;

    private final SubscriptionCache subscriptionCache;

    private final SubscriptionResourceListener subscriptionResourceListener;

    private final ConcurrentMessageListenerContainer<String, String> messageListenerContainer;

    public GalaxyService(ApplicationContext applicationContext, SubscriptionCache subscriptionCache, SubscriptionResourceListener subscriptionResourceListener,
                         ConcurrentMessageListenerContainer<String, String> messageListenerContainer) {
        this.applicationContext = applicationContext;
        this.subscriptionCache = subscriptionCache;
        this.subscriptionResourceListener = subscriptionResourceListener;
        this.messageListenerContainer = messageListenerContainer;
    }

    @PostConstruct
    public void init() {
        if (subscriptionResourceListener != null) {
            try {
                subscriptionResourceListener.start();
            } catch (CouldNotStartInformerException e) {
                log.error(e.getMessage(), e);
                SpringApplication.exit(applicationContext, () -> 1);
            }
        }

        if (messageListenerContainer != null) {
            messageListenerContainer.start();

            log.info("ConcurrentMessageListenerContainer started.");
        }
    }

    @EventListener
    public void handleSubscriptionResourceListenerEvent(ListenerEvent e) {
        if (e.getType() == ListenerEvent.Type.SUBSCRIPTION_RESOURCE_LISTENER) {
            switch (e.getEvent()) {
                case INFORMER_STARTED -> {
                    log.info("Received INFORMER_STARTED event from SubscriptionResourceListener.");

                    subscriptionCache.setHealthy();
                }
                case INFORMER_STOPPED -> {
                    log.error("Received INFORMER_STOPPED event from SubscriptionResourceListener, terminating.");

                    SpringApplication.exit(applicationContext, () -> 2);
                }
            }
        }
    }

    @EventListener
    public void containerStoppedHandler(ContainerStoppedEvent event) {
        log.error("MessageListenerContainer stopped with event {}. Exiting...", event.toString());

        if (messageListenerContainer != null) {
            messageListenerContainer.stop();
        }

        SpringApplication.exit(applicationContext, () -> 3);
    }
}
