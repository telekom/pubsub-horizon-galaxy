package de.telekom.horizon.galaxy.cache;

import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.horizon.galaxy.model.SubscriptionCacheKey;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The {@code SubscriptionCache} class is responsible for caching {@link SubscriptionResource} instances
 * based on the environment and event type. Additionally, it interacts with a {@link SubscriptionCountGaugeCache} to maintain
 * subscription count gauges.
 */
@Component
public class SubscriptionCache {

    private final SubscriptionCountGaugeCache subscriptionCountGaugeCache;
    private final ConcurrentHashMap<SubscriptionCacheKey, ConcurrentHashMap<String, SubscriptionResource>> cache = new ConcurrentHashMap<>();

    public SubscriptionCache(SubscriptionCountGaugeCache subscriptionCountGaugeCache) {
        this.subscriptionCountGaugeCache = subscriptionCountGaugeCache;
    }

    private SubscriptionCacheKey generateSubscriptionCacheKey(String environment, String eventType) {
        return new SubscriptionCacheKey(environment, eventType);
    }

    /**
     * Adds a {@link SubscriptionResource} to the cache, mapped to a {@link SubscriptionCacheKey}.
     * If a {@link SubscriptionResource} did not previously exist under the same {@link SubscriptionCacheKey},
     * the subscription count gauge for this subscription is incremented.
     *
     * @param environment          The environment in which the subscription resource exists.
     * @param eventType            The event type related to the subscription resource.
     * @param subscriptionResource The SubscriptionResource to be added to the cache.
     */
    public void add(String environment, String eventType, SubscriptionResource subscriptionResource) {
        SubscriptionCacheKey key = generateSubscriptionCacheKey(environment, eventType);

        cache.computeIfAbsent(key, e -> new ConcurrentHashMap<>());

        var oldResource = cache.get(key).put(subscriptionResource.getSpec().getSubscription().getSubscriptionId(), subscriptionResource);

        if (oldResource == null) {
            subscriptionCountGaugeCache.getOrCreateGaugeForSubscription(environment, subscriptionResource).getAndIncrement();
        }
    }

    /**
     * Removes the {@link SubscriptionResource} from the cache identified by a {@link SubscriptionCacheKey}.
     * If a {@link SubscriptionResource} previously existed under the same {@link SubscriptionCacheKey},
     * the subscription count gauge for this subscription is decremented.
     *
     * @param environment           The environment in which the subscription resource exists.
     * @param eventType             The event type related to the subscription resource.
     * @param subscriptionResource  The SubscriptionResource to be removed from the cache.
     */
    public void remove(String environment, String eventType, SubscriptionResource subscriptionResource) {
        SubscriptionCacheKey key = generateSubscriptionCacheKey(environment, eventType);

        cache.computeIfAbsent(key, e -> new ConcurrentHashMap<>());
        var oldResource = cache.get(key).remove(subscriptionResource.getSpec().getSubscription().getSubscriptionId());

        if (oldResource != null) {
            subscriptionCountGaugeCache.getOrCreateGaugeForSubscription(environment, subscriptionResource).getAndDecrement();
        }
    }

    /**
     * Retrieves a map of {@link SubscriptionResource} for the specified environment and event type.
     *
     * @param environment   The environment to gather subscriptions from.
     * @param eventType     The type of event for the subscriptions.
     * @return A concurrent map where the keys are the SubscriptionIds and the values are the corresponding {@link SubscriptionResource}.
     * If no such subscriptions exist for the given environment and event type, this method may return <strong>null</strong>.
     */
    public ConcurrentMap<String, SubscriptionResource> getSubscriptionsForEnvironmentAndEventType(String environment, String eventType) {
        SubscriptionCacheKey key = generateSubscriptionCacheKey(environment, eventType);
        return cache.get(key);
    }
}
