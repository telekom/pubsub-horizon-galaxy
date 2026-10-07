// Copyright 2026 Deutsche Telekom AG
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.cache;

/** Signals that subscriptions could not be read; the event must be re-consumed (nack). */
public class SubscriptionLookupException extends RuntimeException {

    public SubscriptionLookupException(String message, Throwable cause) {
        super(message, cause);
    }
}
