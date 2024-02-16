package de.telekom.horizon.galaxy.model;

public record SubscriptionCountGaugeCacheKey(String environment, String eventType, String deliveryType) {
}
