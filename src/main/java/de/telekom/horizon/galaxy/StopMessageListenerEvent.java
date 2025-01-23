// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy;

import lombok.Getter;
import org.springframework.context.ApplicationEvent;

@Getter
public class StopMessageListenerEvent extends ApplicationEvent {
    private final String message;

    public StopMessageListenerEvent(Object source, String message) {
        super(source);
        this.message = message;
    }
}