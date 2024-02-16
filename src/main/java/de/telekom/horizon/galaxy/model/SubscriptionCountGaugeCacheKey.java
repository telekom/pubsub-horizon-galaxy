// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.model;

public record SubscriptionCountGaugeCacheKey(String environment, String eventType, String deliveryType) {
}
