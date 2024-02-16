package de.telekom.horizon.galaxy.service;

import de.telekom.eni.pandora.horizon.kubernetes.InformerStoreInitHandler;
import de.telekom.eni.pandora.horizon.kubernetes.SubscriptionResourceListener;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
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

    private final ApplicationContext context;

    public GalaxyService(@Autowired(required = false) SubscriptionResourceListener subscriptionResourceListener,
                         ConcurrentMessageListenerContainer<String, String> messageListenerContainer,
                         @Autowired(required = false) InformerStoreInitHandler informerStoreInitHandler,
                         ApplicationContext context) {
        this.subscriptionResourceListener = subscriptionResourceListener;
        this.messageListenerContainer = messageListenerContainer;
        this.informerStoreInitHandler = informerStoreInitHandler;
        this.context = context;
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

    @EventListener
    public void containerStoppedHandler(ContainerStoppedEvent event) {
        log.error("MessageListenerContainer stopped with event {}. Exiting...", event.toString());
        if (messageListenerContainer != null) {
            messageListenerContainer.stop();
        }
        SpringApplication.exit(context, () -> -2);
    }
}
