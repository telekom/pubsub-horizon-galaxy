// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.telekom.eni.pandora.horizon.metrics.HorizonMetricsHelper;
import de.telekom.eni.pandora.horizon.model.event.Event;
import de.telekom.eni.pandora.horizon.model.event.EventMessage;
import de.telekom.eni.pandora.horizon.model.event.SubscriptionEventMessage;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Tags;
import lombok.extern.slf4j.Slf4j;
import org.springframework.lang.NonNull;

import java.util.concurrent.ConcurrentHashMap;

import static de.telekom.eni.pandora.horizon.metrics.HorizonMetricsConstants.TAG_EVENT_TYPE;

/**
 * This class is responsible for caching distribution summaries of {@link EventMessage}
 * sizes. An {@link EventMessage} size is the count of bytes of {@link Event} in its JSON representation.
 * Distribution summaries are created on-demand when an {@link EventMessage} is processed,
 * and are identified and retrieved using a key formed by the event type and - if
 * applicable - the SubscriptionId.
 */
@Slf4j
public class PayloadSizeHistogramCache {

    private final HorizonMetricsHelper metricsHelper;

    private final String metricName;

    private final ConcurrentHashMap<String, DistributionSummary> cache;

    private final ObjectMapper objectMapper;

    public PayloadSizeHistogramCache(HorizonMetricsHelper metricsHelper, String metricName, ObjectMapper objectMapper) {
        this.metricsHelper = metricsHelper;
        this.metricName = metricName;
        this.cache = new ConcurrentHashMap<>();
        this.objectMapper = objectMapper;
    }

    /**
     * Gets a {@link DistributionSummary} associated with an event message based on the event type and -
     * if applicable - the SubscriptionId.
     * If the distribution summary does not already exist in the cache, then a new one is registered with
     * the metrics registry and added to the cache.
     *
     * @param message The event message for which to retrieve the {@link DistributionSummary}.
     * @return A {@link DistributionSummary} from the cache, based on the event type and optional SubscriptionId of the {@link EventMessage}.
     */
    private DistributionSummary getOrCreateHistogramForEventMessage(@NonNull EventMessage message) {
        var keyBuilder = new StringBuilder().append(message.getEvent().getType());
        if (message instanceof SubscriptionEventMessage subscriptionEventMessage)
            keyBuilder.append('-').append(subscriptionEventMessage.getSubscriptionId());

        var key = keyBuilder.toString();

        cache.computeIfAbsent(key, k -> DistributionSummary.builder(metricName)
                .publishPercentileHistogram()
                .baseUnit("bytes")
                .tags(buildTagsFromMessage(message))
                .register(metricsHelper.getRegistry()));
        return cache.get(key);
    }


    /**
     * Records the size of the provided {@link EventMessage} by converting its associated {@link Event}
     * to JSON and calculating the length of the JSON string in bytes. The size is then
     * recorded in the {@link DistributionSummary} associated with the event message's
     * event type and optional SubscriptionId.
     *
     * @param message The {@link EventMessage} to record the size of.
     */
    public void recordMessage(EventMessage message) {
        try {
            var jsonStringLength = objectMapper.writeValueAsString(message.getEvent()).getBytes().length;
            getOrCreateHistogramForEventMessage(message).record(jsonStringLength);
        } catch (JsonProcessingException e) {
            log.error("Could not record size of message with id {}", message.getUuid(), e);
        }
    }

    /**
     * Builds a set of {@link Tags} from the provided {@link EventMessage} by including
     * the type of the event and optionally the SubscriptionId if the event message
     * is an instance of {@link SubscriptionEventMessage}.
     *
     * @param message The {@link EventMessage} from which to build the tags.
     * @return A set of {@link Tags} derived from the event message.
     */
    private Tags buildTagsFromMessage(@NonNull EventMessage message) {
        var eventType = message.getEvent().getType();
        var tags = Tags.of(TAG_EVENT_TYPE, eventType);
        return tags;
    }
}
