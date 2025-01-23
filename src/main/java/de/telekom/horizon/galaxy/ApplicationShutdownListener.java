package de.telekom.horizon.galaxy;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.stereotype.Component;

@Component
public class ApplicationShutdownListener implements ApplicationListener<ContextClosedEvent> {

    private final ApplicationEventPublisher applicationEventPublisher;

    public ApplicationShutdownListener(ApplicationEventPublisher applicationEventPublisher) {
        this.applicationEventPublisher = applicationEventPublisher;
    }

    /*
        Will execute even before any @PreStop invocation
     */
    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        var message = "Got shutdown event. Stopping Horizon Galaxy...";

        applicationEventPublisher.publishEvent(new StopGalaxyServiceEvent(this, message));
    }
}