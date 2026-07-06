// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.kafka;

import brave.ScopedSpan;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import de.telekom.eni.pandora.horizon.cache.service.DeDuplicationService;
import de.telekom.eni.pandora.horizon.kafka.event.EventWriter;
import de.telekom.eni.pandora.horizon.kubernetes.resource.Subscription;
import de.telekom.eni.pandora.horizon.metrics.AdditionalFields;
import de.telekom.eni.pandora.horizon.metrics.HorizonMetricsHelper;
import de.telekom.eni.pandora.horizon.model.db.PartialEvent;
import de.telekom.eni.pandora.horizon.model.event.*;
import de.telekom.eni.pandora.horizon.model.http.HeaderConstants;
import de.telekom.eni.pandora.horizon.model.meta.EventRetentionTime;
import de.telekom.eni.pandora.horizon.model.tracing.Constants;
import de.telekom.eni.pandora.horizon.tracing.HorizonTracer;
import de.telekom.horizon.galaxy.cache.PayloadSizeHistogramCache;
import de.telekom.horizon.galaxy.cache.SubscriberCache;
import de.telekom.horizon.galaxy.config.GalaxyConfig;
import de.telekom.horizon.galaxy.model.EvaluationResultStatus;
import de.telekom.horizon.galaxy.filters.FilterEventMessageWrapper;
import de.telekom.horizon.galaxy.filters.Filters;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.support.SendResult;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;

import static de.telekom.eni.pandora.horizon.metrics.HorizonMetricsConstants.METRIC_MULTIPLEXED_EVENTS;

/**
 * The {@code PublishedMessageTask} class is responsible for handling a single {@link PublishedEventMessage}.
 * This class is responsible for deserializing the incoming consumer record, getting the list of recipients
 * for the {@link PublishedEventMessage}, filtering out the event message for each recipient,
 * creating {@link SubscriptionEventMessage} for all recipients, and sending them to Kafka.
 */
@Slf4j
public class PublishedMessageTask implements Callable<CompletableFuture<Void>> {
    private static final Pattern jsonMediaTypeRegex = Pattern.compile("^application/(?:[a-zA-Z0-9]+\\+)?json.*", Pattern.CASE_INSENSITIVE);

    private final ObjectMapper objectMapper;
    private final ConsumerRecord<String, String> consumerRecord;
    private final HorizonTracer tracer;
    private final EventWriter eventWriter;
    private final HorizonMetricsHelper metricsHelper;
    private final SubscriberCache subscriptionCache;
    private final DeDuplicationService deDuplicationService;
    private PublishedEventMessage publishedEventMessage;
    private final PayloadSizeHistogramCache incomingPayloadSizeCache;
    private final PayloadSizeHistogramCache outgoingPayloadSizeHistogramCache;
    private final GalaxyConfig galaxyConfig;

    public PublishedMessageTask(ConsumerRecord<String, String> consumerRecord, PublishedMessageTaskFactory factory) {
        this.consumerRecord = consumerRecord;

        this.tracer = factory.getTracer();
        this.eventWriter = factory.getEventWriter();
        this.metricsHelper = factory.getMetricsHelper();
        this.subscriptionCache = factory.getSubscriptionCache();
        this.deDuplicationService = factory.getDeDuplicationService();
        this.incomingPayloadSizeCache = factory.getIncomingPayloadSizeCache();
        this.outgoingPayloadSizeHistogramCache = factory.getOutgoingPayloadSizeHistogramCache();
        this.objectMapper = factory.getObjectMapper();
        this.galaxyConfig = factory.getGalaxyConfig();
    }

    @Override
    public CompletableFuture<Void> call() {
        //Start main span for published-message-task
        var span = tracer.startSpanFromKafkaHeaders("consume published message", consumerRecord.headers());
        try (var ignored = tracer.withSpanInScope(span)) {
            //Get PublishedEventMessage from consumerRecord
            try {
                publishedEventMessage = objectMapper.readValue(consumerRecord.value(), PublishedEventMessage.class);
            } catch (JsonProcessingException e) {
                log.error("JsonProcessingException occurred while parsing published event message with key {}!", consumerRecord.key(), e);
                // Better to move to DLQ for messages that are not parseable
                return CompletableFuture.completedFuture(null);
            }

            try (
                    var ignored1 = MDC.putCloseable("UUID", publishedEventMessage.getUuid());
                    var ignored2 = MDC.putCloseable("EventId", publishedEventMessage.getEvent().getId())
            ) {
                recordIncomingPayloadSize();

                log.info("Created Task from ConsumerRecord.");
                final var event = publishedEventMessage.getEvent();
                final var eventType = event.getType();
                final var subscriptionResources = subscriptionCache.getSubscriptionsForEnvironmentAndEventType(publishedEventMessage.getEnvironment(), eventType);
                if (subscriptionResources == null) {
                    log.info("No recipients found for event. Skipping multiplexing.");
                    return CompletableFuture.completedFuture(null);
                }

                final var eventData = event.getData();
                final var dataContentType = event.getDataContentType();
                final var messagePublishingTasks = new ArrayList<CompletableFuture<SendResult<String, String>>>(subscriptionResources.size());
                final var eventJsonPayload = eventData != null && (dataContentType == null || jsonMediaTypeRegex.matcher(dataContentType.trim()).matches()) ?
                        parseEventData(eventData)
                        : null;
                for (var subscriptionResource : subscriptionResources) {
                    final var subscription = subscriptionResource.getSpec().getSubscription();
                    final var subscriptionId = subscription.getSubscriptionId();
                    if (deDuplicationService.isDuplicate(publishedEventMessage, subscriptionId)) {
                        continue;
                    }

                    log.info("Applying filters.");
                    final var filteredEventMessage = getFilteredEventMessage(subscription, eventJsonPayload, galaxyConfig);

                    log.info("Creating SubscriptionEventMessage for subscription {}", subscriptionId);
                    final var subscriptionEventMessage = createSubscriptionEventMessage(filteredEventMessage, event, subscription);

                    log.info("Sending SubscriptionEventMessage for subscription {}.", subscriptionId);
                    final var messagePublishingTask = sendMessageToKafka(subscriptionEventMessage, filteredEventMessage);
                    messagePublishingTasks.add(messagePublishingTask);
                }

                return CompletableFuture.allOf(messagePublishingTasks.toArray(new CompletableFuture[0]));
            }
        } finally {
            span.finish();
        }
    }

    // should probably be part of the parent library, as a copy-constructor
    private Event copyEvent(final Event originalEvent, final Object data) {
        final var copy = new Event();
        copy.setId(originalEvent.getId());
        copy.setType(originalEvent.getType());
        copy.setSource(originalEvent.getSource());
        copy.setSpecVersion(originalEvent.getSpecVersion());
        copy.setDataContentType(originalEvent.getDataContentType());
        copy.setDataRef(originalEvent.getDataRef());
        copy.setTime(originalEvent.getTime());
        copy.setData(data);

        return copy;
    }

    private CompletableFuture<SendResult<String, String>> sendMessageToKafka(
            final SubscriptionEventMessage subscriptionEventMessage,
            final FilterEventMessageWrapper filteredEventMessage
    ) {
        final Callable<CompletableFuture<SendResult<String, String>>> sendMessageTask = () -> {
            final var subscriptionId = subscriptionEventMessage.getSubscriptionId();
            var multiplexSpan = tracer.startScopedSpan("multiplex message");
            recordOutgoingPayloadSize(subscriptionEventMessage);
            enrichTracing(multiplexSpan, subscriptionEventMessage, filteredEventMessage);

            CompletableFuture<SendResult<String, String>> result;
            try {
                result = sendOutgoingMessage(filteredEventMessage, subscriptionEventMessage);
                log.info("Successfully sent SubscriptionEventMessage for subscription {}.", subscriptionId);
                trackEventForDeduplication(subscriptionEventMessage);
            } catch (JsonProcessingException e) {
                log.error("An error occurred while sending SubscriptionEventMessage for subscription {}!", subscriptionId, e);
                result = sendFailedStatusMessage(subscriptionEventMessage);
                trackEventForDeduplication(subscriptionEventMessage);
            } finally {
                var tags = metricsHelper.buildTagsFromSubscriptionEventMessage(subscriptionEventMessage);
                metricsHelper.getRegistry().counter(METRIC_MULTIPLEXED_EVENTS, tags).increment();
                multiplexSpan.finish();
            }

            return result;
        };

        try {
            return tracer.withCurrentContext(sendMessageTask).call();
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Tracks the {@link SubscriptionEventMessage} as multiplexed.
     * Therefore, if a previous {@link PublishedMessageTask} should finish unsuccessfully the {@link SubscriptionEventMessage}
     * will not be created again (which is intended).
     *
     * @param subscriptionEventMessage The {@link SubscriptionEventMessage} containing {@link PublishedEventMessage} UUID and SubscriptionId
     */
    private void trackEventForDeduplication(SubscriptionEventMessage subscriptionEventMessage) {
        log.debug("Tracking SubscriptionEventMessage in deduplication cache for subscription {}.", subscriptionEventMessage.getSubscriptionId());
        deDuplicationService.track(subscriptionEventMessage);
    }


    /**
     * Records the outgoing payload size for the given {@link SubscriptionEventMessage}.
     *
     * @param subscriptionEventMessage The {@link SubscriptionEventMessage} for which to record the outgoing payload size.
     */
    private void recordOutgoingPayloadSize(SubscriptionEventMessage subscriptionEventMessage) {
        outgoingPayloadSizeHistogramCache.recordMessage(subscriptionEventMessage);
    }

    /**
     * Records the incoming payload size for the published event message.
     */
    private void recordIncomingPayloadSize() {
        incomingPayloadSizeCache.recordMessage(publishedEventMessage);
    }

    /**
     * Creates a {@link StatusMessage} indicating the event delivery has failed and sends this {@link Status#FAILED} message <br>synchronously</br> to Kafka.
     * If an error occurs during this process, it is logged, and a new {@link RuntimeException} is thrown.
     *
     * @param subscriptionEventMessage the event message that failed to be delivered
     * @return CompletableFuture to track the status of the async publishing of the message
     * @throws RuntimeException thrown if an error occurs while sending the {@link Status#FAILED} status message
     */
    private CompletableFuture<SendResult<String, String>> sendFailedStatusMessage(SubscriptionEventMessage subscriptionEventMessage) {
        var event = subscriptionEventMessage.getEvent();
        var partialEvent = new PartialEvent(event.getId(), event.getType(), event.getTime());
        var statusMessage = new StatusMessage(subscriptionEventMessage.getUuid(), partialEvent, subscriptionEventMessage.getSubscriptionId(), Status.FAILED, subscriptionEventMessage.getDeliveryType());

        try {
            return eventWriter.send(Objects.requireNonNullElse(subscriptionEventMessage.getEventRetentionTime(), EventRetentionTime.DEFAULT).getTopic(), statusMessage, tracer);
        } catch (JsonProcessingException ex) {
            log.error("JsonProcessingException occurred while handling the exception from the multiplex task! Nack-ing event {}. May lead into nack-loop...", statusMessage, ex);
            throw new RuntimeException(ex);
        }
    }

    /**
     * Adds additional tracing tags such as multiplexed-from, subscriber-id, the result
     * of the selection filter, and filter traceback to the multiplexSpan.
     *
     * @param multiplexSpan            The span in which this method will add additional tracing info.
     * @param subscriptionEventMessage The message whose delivery was unsuccessful.
     * @param subscriptionFilter       Contains information about the filter applied on the message for the subscription.
     */
    private void enrichTracing(ScopedSpan multiplexSpan, SubscriptionEventMessage subscriptionEventMessage, FilterEventMessageWrapper subscriptionFilter) {
        multiplexSpan.tag(Constants.MULTIPLEXED_FROM, publishedEventMessage.getUuid());
        multiplexSpan.tag(Constants.SUBSCRIBER_ID, (String) subscriptionEventMessage.getAdditionalFields().get("subscriber-id"));

        multiplexSpan.tag(Constants.SELECTION_FILTER_RESULT, subscriptionFilter.getEvaluationResultStatus().toString());
        if (!subscriptionFilter.isSuccessful()) {
            multiplexSpan.tag(Constants.SELECTION_FILTER_TRACE, subscriptionFilter.getEvaluationResult().toString());
        }

        tracer.addTagsToSpanFromEventMessage(multiplexSpan, subscriptionEventMessage);
        tracer.addTagsToSpanFromSubscriptionEventMessage(multiplexSpan, subscriptionEventMessage);
    }

    /**
     * This method sends the {@link SubscriptionEventMessage} <b>asynchronously</b> to kafka  if the filter was successful,
     * else creates a {@link Status#DROPPED} {@link StatusMessage} and sends it to kafka.
     * <p>
     * The messages are filtered according to the subscriptionFilter,
     * and then they are sent to the relevant topics. Performance metrics
     * are recorded and used for observation.
     *
     * @param subscriptionFilter       the applied filters of the {@link SubscriptionEventMessage}, unsuccessful or successful
     * @param subscriptionEventMessage the event message sent to the kafka (if not dropped)
     * @return CompletableFuture       tracks the status of the async publishing of the message
     * @throws JsonProcessingException thrown when there is a problem parsing the payload
     */
    private CompletableFuture<SendResult<String, String>> sendOutgoingMessage(FilterEventMessageWrapper subscriptionFilter, SubscriptionEventMessage subscriptionEventMessage) throws JsonProcessingException {
        var properties = tracer.getCurrentTracingHeaders();

        IdentifiableMessage outgoingMessage;
        if (subscriptionFilter.isSuccessful()) {
            subscriptionEventMessage.setStatus(Status.PROCESSED);
            subscriptionEventMessage.getAdditionalFields().put(AdditionalFields.SELECTION_FILTER_RESULT.value, subscriptionFilter.getEvaluationResultStatus().toString());
            subscriptionEventMessage.getAdditionalFields().putAll(properties);

            outgoingMessage = subscriptionEventMessage;
        } else {
            var droppedMessage = new StatusMessage();
            droppedMessage.setStatus(Status.DROPPED);
            droppedMessage.setUuid(subscriptionEventMessage.getUuid());
            droppedMessage.setSubscriptionId(subscriptionEventMessage.getSubscriptionId());
            droppedMessage.setAdditionalFields(subscriptionEventMessage.getAdditionalFields());
            droppedMessage.getAdditionalFields().put(AdditionalFields.SELECTION_FILTER_RESULT.value, subscriptionFilter.getEvaluationResultStatus().toString());
            droppedMessage.getAdditionalFields().put(AdditionalFields.SELECTION_FILTER_TRACE.value, subscriptionFilter.getEvaluationResult());
            droppedMessage.getAdditionalFields().putAll(properties);
            droppedMessage.setDeliveryType(subscriptionEventMessage.getDeliveryType());

            var eventData = new PartialEvent(subscriptionEventMessage.getEvent().getId(), subscriptionEventMessage.getEvent().getType());
            eventData.setTime(subscriptionEventMessage.getEvent().getTime());
            droppedMessage.setEvent(eventData);

            outgoingMessage = droppedMessage;
        }

        return eventWriter.send(Objects.requireNonNullElse(subscriptionEventMessage.getEventRetentionTime(), EventRetentionTime.DEFAULT).getTopic(), outgoingMessage, tracer);
    }

    private FilterEventMessageWrapper getFilteredEventMessage(final Subscription subscription, final JsonNode eventJsonPayload, final GalaxyConfig galaxyConfig) {
        var filterSpan = tracer.startScopedDebugSpan("apply filters");
        try {
            return Filters.filterDataForRecipient(subscription, eventJsonPayload, galaxyConfig);
        } finally {
            filterSpan.finish();
        }
    }

    /**
     * Parses the provided event data into a JsonNode.
     * <p>
     * If parsing fails due to an {@link IllegalArgumentException} or {@link JsonProcessingException}, null is returned.
     * If the resulting JsonNode is an instance of {@link MissingNode}, indicating an unsuccessful parse, null is also returned.
     *
     * @param eventData The event data to be parsed. It can be either a String representation of JSON or any other object.
     * @return A JsonNode representing the parsed event data, or {@code null} if parsing fails.
     */
    private JsonNode parseEventData(Object eventData) {
        JsonNode jsonEventData = null;
        try {
            if (eventData instanceof String eventDataString) {
                jsonEventData = objectMapper.readTree(eventDataString);
            } else {
                jsonEventData = objectMapper.valueToTree(eventData);
            }
        } catch (IllegalArgumentException | JsonProcessingException e) {
            log.error(e.getMessage());
        }

        if (jsonEventData instanceof MissingNode) {
            return null;
        }

        return jsonEventData;
    }

    private SubscriptionEventMessage createSubscriptionEventMessage(
            final FilterEventMessageWrapper filteredEventMessage,
            final Event event,
            final Subscription subscription
    ) {
        final var subscriptionId = subscription.getSubscriptionId();
        final var multiplexedEvent = filteredEventMessage.getEvaluationResultStatus() == EvaluationResultStatus.MATCH
                ? copyEvent(event, filteredEventMessage.getFilteredPayload())
                : event;

        final var deliveryType = DeliveryType.valueOf(subscription.getDeliveryType().toUpperCase());
        Map<String, Object> additionalFields = new HashMap<>();
        if (publishedEventMessage.getAdditionalFields() != null) {
            additionalFields.putAll(publishedEventMessage.getAdditionalFields());
        }

        final var httpHeaders = new HashMap<String, List<String>>();
        if (publishedEventMessage.getHttpHeaders() != null) {
            httpHeaders.putAll(publishedEventMessage.getHttpHeaders());
        }

        final var subscriberId = subscription.getSubscriberId();
        additionalFields.put("subscriber-id", subscriberId);

        if (deliveryType.equals(DeliveryType.CALLBACK)) {
            additionalFields.put("callback-url", subscription.getCallback());

            httpHeaders.put(HeaderConstants.X_EVENT_ID, List.of(multiplexedEvent.getId()));
            httpHeaders.put(HeaderConstants.X_EVENT_TYPE, List.of(multiplexedEvent.getType()));
            httpHeaders.put(HeaderConstants.X_PUBSUB_PUBLISHER_ID, List.of(subscription.getPublisherId()));
            httpHeaders.put(HeaderConstants.X_PUBSUB_SUBSCRIBER_ID, List.of(subscriberId));
            httpHeaders.put(HeaderConstants.X_SUBSCRIPTION_ID, List.of(subscriptionId));
        }

        final var retentionTimeStrOrNull = subscription.getEventRetentionTime();
        final var eventRetentionTime = Objects.nonNull(retentionTimeStrOrNull)
                ? EventRetentionTime.fromString(retentionTimeStrOrNull.toUpperCase())
                : EventRetentionTime.DEFAULT;

        return new SubscriptionEventMessage(
                multiplexedEvent,
                publishedEventMessage.getEnvironment(),
                deliveryType,
                subscriptionId,
                publishedEventMessage.getUuid(),
                eventRetentionTime,
                subscription.getAppliedScopes(),
                additionalFields,
                httpHeaders
        );
    }
}
