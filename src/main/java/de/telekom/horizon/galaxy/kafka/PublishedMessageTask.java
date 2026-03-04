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
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
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
import de.telekom.horizon.galaxy.model.PublishedMessageTaskResult;
import de.telekom.horizon.galaxy.filters.FilterEventMessageWrapper;
import de.telekom.horizon.galaxy.filters.Filters;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jetbrains.annotations.NotNull;
import org.slf4j.MDC;
import org.springframework.kafka.support.SendResult;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
                return CompletableFuture.completedFuture(null);
            }

            try(
                    var ignored1 = MDC.putCloseable("UUID", publishedEventMessage.getUuid());
                    var ignored2 = MDC.putCloseable("EventId", publishedEventMessage.getEvent().getId())
            ) {
                recordIncomingPayloadSize();

                log.info("Created Task from ConsumerRecord.");

                //Retrieve recipients for event and remove duplicates
                var recipients = getRecipientsForPublishedEventMessage(publishedEventMessage);

                if (recipients.isEmpty()) {
                    log.info("No recipients found for event. Skipping multiplexing.");
                    return CompletableFuture.completedFuture(null);
                }

                log.info("Found {} recipients for event.", recipients.size());

                //Apply response- and selection-filter
                log.info("Applying filters.");
                Map<String, FilterEventMessageWrapper> filteredEventMessagesPerRecipient = getFilteredEventMessagesPerRecipient(recipients, galaxyConfig);

                //Create subscriptionEventMessages for all recipients
                Map<String, SubscriptionEventMessage> subscriptionEventMessagesMap;
                try {
                    subscriptionEventMessagesMap = createSubscriptionEventMessages(recipients, filteredEventMessagesPerRecipient, publishedEventMessage);
                } catch (Exception e) {
                    log.error("An unknown error occurred while handling event.", e);

                    //We should send these events to a dead-letter-topic in future
                    return CompletableFuture.completedFuture(null);
                }

                //Send to Kafka
                return sendMessagesToKafka(subscriptionEventMessagesMap, filteredEventMessagesPerRecipient);
            }
        } finally {
            span.finish();
        }
    }

    /**
     * Sends messages to Kafka for each {@link SubscriptionEventMessage}.
     *
     * @param subscriptionEventMessagesMap       A map of SubscriptionId to {@link SubscriptionEventMessage}.
     * @param filteredEventMessagesPerRecipient  A map of SubscriptionId to {@link FilterEventMessageWrapper}.
     * @return A list of CompletableFutures representing the asynchronous sending of messages to kafka.
     */
    private CompletableFuture<Void> sendMessagesToKafka(
            Map<String, SubscriptionEventMessage> subscriptionEventMessagesMap,
            Map<String, FilterEventMessageWrapper> filteredEventMessagesPerRecipient
    ) {
        // todo: remove before merging: review question - shall we keep it the Go way here or rather do the Java-way i.e. toArray(CompletableFuture[0]) in CompletableFuture.allOf below?
        var messagePublishingTasks = new CompletableFuture[subscriptionEventMessagesMap.size()];
        var i = 0;
        for (var entry : subscriptionEventMessagesMap.entrySet()) {
            log.info("Sending SubscriptionEventMessage for subscription {}.", entry.getKey());
            try {
                messagePublishingTasks[i] = sendMessageToKafka(entry.getValue(), filteredEventMessagesPerRecipient);
                i++;
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        return CompletableFuture.allOf(messagePublishingTasks);
    }

    /**
     * Sends either an {@link SubscriptionEventMessage} or an ({@link Status#FAILED}|{@link Status#DROPPED} {@link StatusMessage} in a new Task to Kafka for the given {@link SubscriptionEventMessage}.
     *
     * @param subscriptionEventMessage          The SubscriptionEventMessage to send.
     * @param filteredEventMessagesPerRecipient A map of SubscriptionId to {@link FilterEventMessageWrapper}.
     * @return CompletableFuture                Tracks the status of the async publishing of the message
     */
    private CompletableFuture<SendResult<String, String>> sendMessageToKafka(
            SubscriptionEventMessage subscriptionEventMessage,
            Map<String, FilterEventMessageWrapper> filteredEventMessagesPerRecipient
    ) throws Exception {
        final Callable<CompletableFuture<SendResult<String, String>>> sendMessage = () -> {
            var multiplexSpan = tracer.startScopedSpan("multiplex message");

            var subscriptionId = subscriptionEventMessage.getSubscriptionId();
            var subscriptionFilter = filteredEventMessagesPerRecipient.get(subscriptionId);

            recordOutgoingPayloadSize(subscriptionEventMessage);
            enrichTracing(multiplexSpan, subscriptionEventMessage, subscriptionFilter);

            CompletableFuture<SendResult<String, String>> result;
            try {
                result = sendOutgoingMessage(subscriptionFilter, subscriptionEventMessage);
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

        return tracer.withCurrentContext(sendMessage).call();
    }

    /**
     * Tracks the {@link SubscriptionEventMessage} as multiplexed.
     * Therefore, if a previous {@link PublishedMessageTask} should finish unsuccessfully the {@link SubscriptionEventMessage}
     * will not be created again (which is intended).
     *
     * @param subscriptionEventMessage  The {@link SubscriptionEventMessage} containing {@link PublishedEventMessage} UUID and SubscriptionId
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
     * @param multiplexSpan             The span in which this method will add additional tracing info.
     * @param subscriptionEventMessage  The message whose delivery was unsuccessful.
     * @param subscriptionFilter Contains information about the filter applied on the message for the subscription.
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

    /**
     * Filters event messages for each recipient according to predefined criteria.
     * <p>
     * This method applies filters to the payload of event messages based on the recipients'
     * preferences defined in the {@link SubscriptionResource}. The filtering process might
     * remove parts of the payload or transforms it.
     *
     * @param recipients a collection containing the {@link SubscriptionResource} for each recipient
     * @return a Map where the key is the SubscriptionId and the value is the filtered event message
     * @see FilterEventMessageWrapper
     */
    @NotNull
    private Map<String, FilterEventMessageWrapper> getFilteredEventMessagesPerRecipient(List<SubscriptionResource> recipients, GalaxyConfig galaxyConfig) {
        var filterSpan = tracer.startScopedDebugSpan("apply filters");
        Map<String, FilterEventMessageWrapper> filteredEventMessagesPerRecipient = new HashMap<>();
        try {
            Object eventData = publishedEventMessage.getEvent().getData();
            JsonNode jsonEventDataOrNull = null;

            var dataContentType = publishedEventMessage.getEvent().getDataContentType();
            var jsonMediaTypeRegexPattern = Pattern.compile("^application/(?:[a-zA-Z0-9]+\\+)?json.*", Pattern.CASE_INSENSITIVE);

            if (eventData != null && (dataContentType == null || jsonMediaTypeRegexPattern.matcher(dataContentType.trim()).matches())) {
                jsonEventDataOrNull = parseEventData(eventData);
            }

            filteredEventMessagesPerRecipient = Filters.filterDataForRecipients(recipients, jsonEventDataOrNull, galaxyConfig);
        } finally {
            filterSpan.finish();
        }
        return filteredEventMessagesPerRecipient;
    }

    /**
     * Retrieves a list of recipients {@link SubscriptionResource} for a given {@link PublishedEventMessage}.
     * <p>
     * This method queries the {@link SubscriberCache} to obtain subscriptions associated with the specified environment and event type in the given {@link PublishedEventMessage}.
     * It then filters the subscriptions to exclude duplicates based on the published event message UUID and the SubscriptionId.
     * The resulting list represents unique recipients where the message has not been multiplexed yet.
     *
     * @param publishedEventMessage The {@link PublishedEventMessage} for which recipients are to be retrieved.
     * @return A list of {@link SubscriptionResource} objects representing the recipients for the given event message.
     *         If no matching subscriptions are found, an empty list is returned.
     *
     * @throws NullPointerException if the provided PublishedEventMessage is null.
     * @see DeDuplicationService
     */
    private List<SubscriptionResource> getRecipientsForPublishedEventMessage(PublishedEventMessage publishedEventMessage) {
        String eventType = publishedEventMessage.getEvent().getType();
        var subscriptionsForEnvironmentAndType = subscriptionCache.getSubscriptionsForEnvironmentAndEventType(publishedEventMessage.getEnvironment(), eventType);
        return subscriptionsForEnvironmentAndType != null ?
                subscriptionsForEnvironmentAndType.stream()
                        .filter(subscriptionResource -> !deDuplicationService.isDuplicate(publishedEventMessage, subscriptionResource.getSpec().getSubscription().getSubscriptionId()))
                        .toList() :
                Collections.emptyList();
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

    /**
     * Creates {@link SubscriptionEventMessage} instances for each recipient.
     * <p>
     * This method iterates through the list of recipients and creates a {@link SubscriptionEventMessage} for each.
     * It copies the original event, applies filtered data if a response filter was used, sets the additional fields,
     * and constructs the {@link SubscriptionEventMessage} with the delivery type, SubscriberId,
     * callbackUrl, and custom headers.
     *
     * @param recipients                         A list of {@link SubscriptionResource} objects representing the recipients for the event message.
     * @param filteredEventDataPerSubscriptionId A map filtered event data.
     * @param publishedEventMessage              The {@link PublishedEventMessage} for which {@link SubscriptionEventMessage}s are to be created of.
     * @return A map where the key is the SubscriptionId, and the value is the corresponding {@link SubscriptionEventMessage} for each recipient.
     *
     * @see FilterEventMessageWrapper
     */
    private Map<String, SubscriptionEventMessage> createSubscriptionEventMessages(List<SubscriptionResource> recipients, Map<String, FilterEventMessageWrapper> filteredEventDataPerSubscriptionId, PublishedEventMessage publishedEventMessage) throws Exception {
        var subscriptionEventMessagesPerSubscriptionId = new HashMap<String, SubscriptionEventMessage>();

        for (SubscriptionResource recipient : recipients) {
            log.info("Creating SubscriptionEventMessage for subscription {}", recipient.getSpec().getSubscription().getSubscriptionId());

            //Create EventCopy
            Event eventCopy;
            try {
                eventCopy = objectMapper.readValue(objectMapper.writeValueAsString(publishedEventMessage.getEvent()), Event.class);
            } catch (Exception e) {
                throw new Exception(e.getMessage());
            }

            //If response filter was applied use stripped data
            String subscriptionId = recipient.getSpec().getSubscription().getSubscriptionId();
            var evaluationStatus = filteredEventDataPerSubscriptionId.get(subscriptionId).getEvaluationResultStatus();

            if (evaluationStatus == EvaluationResultStatus.MATCH) {
                eventCopy.setData(filteredEventDataPerSubscriptionId.get(subscriptionId).getFilteredPayload());
            }

            var deliveryType = DeliveryType.valueOf(recipient.getSpec().getSubscription().getDeliveryType().toUpperCase());

            Map<String, Object> additionalFields = new HashMap<>();
            if (publishedEventMessage.getAdditionalFields() != null) {
                additionalFields.putAll(publishedEventMessage.getAdditionalFields());
            }

            var httpHeaders = new HashMap<String, List<String>>();
            if (publishedEventMessage.getHttpHeaders() != null) {
                httpHeaders.putAll(publishedEventMessage.getHttpHeaders());
            }

            var subscriberId = recipient.getSpec().getSubscription().getSubscriberId();
            additionalFields.put("subscriber-id", subscriberId);

            if (deliveryType.equals(DeliveryType.CALLBACK)) {
                additionalFields.put("callback-url", recipient.getSpec().getSubscription().getCallback());

                httpHeaders.put(HeaderConstants.X_EVENT_ID, List.of(eventCopy.getId()));
                httpHeaders.put(HeaderConstants.X_EVENT_TYPE, List.of(eventCopy.getType()));
                httpHeaders.put(HeaderConstants.X_PUBSUB_PUBLISHER_ID, List.of(recipient.getSpec().getSubscription().getPublisherId()));
                httpHeaders.put(HeaderConstants.X_PUBSUB_SUBSCRIBER_ID, List.of(subscriberId));
                httpHeaders.put(HeaderConstants.X_SUBSCRIPTION_ID, List.of(subscriptionId));
            }

            var retentionTimeStrOrNull = recipient.getSpec().getSubscription().getEventRetentionTime();
            var eventRetentionTime = Objects.nonNull(retentionTimeStrOrNull) ? EventRetentionTime.fromString(retentionTimeStrOrNull.toUpperCase()) : EventRetentionTime.DEFAULT;

            //Event event, String environment, DeliveryType deliveryType, String subscriptionId, String multiplexedFrom, EventRetentionTime eventRetentionTime, List<String> appliedScopes, Map<String, Object> additionalFields, Map<String, List<String>> httpHeaders
            var subscriptionEventMessage = new SubscriptionEventMessage(eventCopy, publishedEventMessage.getEnvironment(), deliveryType, subscriptionId, publishedEventMessage.getUuid(), eventRetentionTime, recipient.getSpec().getSubscription().getAppliedScopes(), additionalFields, httpHeaders);
            subscriptionEventMessagesPerSubscriptionId.put(subscriptionId, subscriptionEventMessage);
        }

        return subscriptionEventMessagesPerSubscriptionId;
    }

}
