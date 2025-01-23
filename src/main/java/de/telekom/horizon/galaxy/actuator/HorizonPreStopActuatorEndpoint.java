// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.actuator;

import de.telekom.horizon.galaxy.StopMessageListenerEvent;
import de.telekom.horizon.galaxy.config.GalaxyConfig;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
@Endpoint(id = "horizon-prestop")
public class HorizonPreStopActuatorEndpoint {

    private final GalaxyConfig config;

    private final ApplicationEventPublisher applicationEventPublisher;

    public HorizonPreStopActuatorEndpoint(GalaxyConfig config, ApplicationEventPublisher applicationEventPublisher) {
        this.config = config;
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @WriteOperation
    public void handlePreStop() {
        var event = new StopMessageListenerEvent(this, "Got PreStop request. Request to stop message listener container...");
        applicationEventPublisher.publishEvent(event);

        try {
            Thread.sleep(Instant.ofEpochSecond(config.getPreStopWaitTimeInSeconds()).toEpochMilli());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}