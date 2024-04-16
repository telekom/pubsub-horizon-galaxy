// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.service;

import de.telekom.eni.pandora.horizon.kubernetes.InformerStoreInitHandler;
import de.telekom.eni.pandora.horizon.kubernetes.SubscriptionResourceListener;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ExitCodeEvent;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.event.ContainerStoppedEvent;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class GalaxyService {

    private final SubscriptionResourceListener subscriptionResourceListener;

    private final ConcurrentMessageListenerContainer<String, String> messageListenerContainer;

    private final InformerStoreInitHandler informerStoreInitHandler;

    private final ApplicationContext applicationContext;

    public GalaxyService(@Autowired(required = false) SubscriptionResourceListener subscriptionResourceListener,
                         ConcurrentMessageListenerContainer<String, String> messageListenerContainer,
                         @Autowired(required = false) InformerStoreInitHandler informerStoreInitHandler,
                         ApplicationContext applicationContext) {
        this.subscriptionResourceListener = subscriptionResourceListener;
        this.messageListenerContainer = messageListenerContainer;
        this.informerStoreInitHandler = informerStoreInitHandler;
        this.applicationContext = applicationContext;
    }

    @PostConstruct
    public void init() {
        if (subscriptionResourceListener != null) {
            subscriptionResourceListener.start();

            log.info("SubscriptionResourceListener started.");

            (new Thread(() -> {
                log.info("Waiting until Subscription resources are fully synced...");

                while (!informerStoreInitHandler.isFullySynced()) {
                    try {
                        // Will be cleaned up in future
                        Thread.sleep(1000L);
                    } catch (InterruptedException var6) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }

                startMessageListenerContainer();
            })).start();
        } else {
            startMessageListenerContainer();
        }
    }

    private void startMessageListenerContainer() {
        if (messageListenerContainer != null) {
            messageListenerContainer.start();

            log.info("ConcurrentMessageListenerContainer started.");
        }
    }

    /**
     * Handles the application stopping event by stopping the Kafka message listener container.
     */
    @EventListener(classes = {ExitCodeEvent.class, ContextClosedEvent.class})
    public void applicationStoppedHandler() {
        if (messageListenerContainer != null && messageListenerContainer.isRunning()) {
            messageListenerContainer.stop();

            log.info("Kafka message listener container stopped.");
        }
    }

    /**
     * Handles the event when the Kafka message listener container is stopped.
     * This method initiates the application exit process when the Kafka message listener container has been stopped.
     */
    @EventListener(classes = {ContainerStoppedEvent.class})
    public void containerStoppedHandler() {
        log.error("MessageListenerContainer stopped. Exiting...");

        System.exit(SpringApplication.exit(applicationContext, () -> 1));
    }
}
