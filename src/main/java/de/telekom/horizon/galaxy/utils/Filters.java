// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.telekom.eni.pandora.horizon.kubernetes.resource.Subscription;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionTrigger;
import de.telekom.jsonfilter.operator.EvaluationResult;
import de.telekom.jsonfilter.operator.Operator;
import de.telekom.jsonfilter.operator.comparison.ComparisonOperator;
import de.telekom.jsonfilter.operator.comparison.ComparisonOperatorEnum;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.*;

/**
 * Utility class responsible for filtering data for recipients based on
 * subscription filters including selection filter and advanced selection filter.
 * <br><br>
 * This class performs various operations, including the creation of
 * filtered scopes and consumer filter events for subscriptions, and
 * checking for filter existence and applicability for a given subscription.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Filters {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Filter incoming data for each recipient based on their subscription.
     *
     * @param recipients     list of recipient subscriptions.
     * @param jsonEventDataOrNull  incoming data in optional JsonNode format.
     * @return map of subscriptionIds mapped to filtered event data for each subscription.
     */
    public static Map<String, FilterEventMessageWrapper> filterDataForRecipients(List<SubscriptionResource> recipients, JsonNode jsonEventDataOrNull) {
        Map<String, FilterEventMessageWrapper> filteredEventWrapperPerSubscriptionId = new HashMap<>();

        recipients.forEach(recipient -> {
            var subscription = recipient.getSpec().getSubscription();
            filteredEventWrapperPerSubscriptionId.put(subscription.getSubscriptionId(), getFilteredEventDataForSubscription(subscription, jsonEventDataOrNull));
        });

        return filteredEventWrapperPerSubscriptionId;
    }

    /**
     * Get the filtered event data for a specific subscription.
     *
     * @param subscription         specific subscription to be applied to the data.
     * @param jsonEventDataOrNull  incoming data in optional JsonNode format.
     * @return filtered event data for the specific subscription.
     */
    private static FilterEventMessageWrapper getFilteredEventDataForSubscription(Subscription subscription, JsonNode jsonEventDataOrNull) {
        log.info("Filtering event data for subscription: {}", subscription.getSubscriptionId());

        //Check if any filter is present
        if (!hasScopeOrConsumerFilter(subscription)) {
            log.info("No filter present for subscription: {}", subscription.getSubscriptionId());
            return FilterEventMessageWrapper.withNoFilter();
        }

        //Check if the payload is valid JSON
        if (jsonEventDataOrNull == null) {
            log.info("Payload is not valid JSON for subscription: {}", subscription.getSubscriptionId());
            return FilterEventMessageWrapper.withPayloadInvalidError();
        }

        //Check if the scope filter matches
        var scopedFilterResult = applyFilter(subscription.getPublisherTrigger(), jsonEventDataOrNull);
        if (!scopedFilterResult.getLeft().isMatch()) {
            log.info("Scope filter did not match for subscription: {}", subscription.getSubscriptionId());
            return FilterEventMessageWrapper.withScopeError(scopedFilterResult.getLeft());
        }

        //Check if the consumer filter matches
        var consumerFilterResult = applyFilter(subscription.getTrigger(), scopedFilterResult.getRight());
        if (!consumerFilterResult.getLeft().isMatch()) {
            log.info("Consumer filter did not match for subscription: {}", subscription.getSubscriptionId());
            return FilterEventMessageWrapper.withConsumerError(consumerFilterResult.getLeft());
        }

        log.info("All filters matched for subscription: {}", subscription.getSubscriptionId());
        return FilterEventMessageWrapper.withMatch(consumerFilterResult.getRight());
    }

    /**
     * Check if a subscription has either a scope filter or a consumer filter present.
     *
     * @param subscription   subscription to check.
     * @return true if a scope filter or a consumer filter is present; false otherwise.
     */
    private static boolean hasScopeOrConsumerFilter(Subscription subscription) {
        var hasScopeFilter = subscription.getPublisherTrigger() != null && (subscription.getPublisherTrigger().getAdvancedSelectionFilter() != null || subscription.getPublisherTrigger().getResponseFilter() != null);
        var hasConsumerFilter = subscription.getTrigger() != null && (subscription.getTrigger().getSelectionFilter() != null || subscription.getTrigger().getAdvancedSelectionFilter() != null || subscription.getTrigger().getResponseFilter() != null);
        return hasScopeFilter || hasConsumerFilter;
    }

    /**
     * Apply a filter to the provided JSON event data based on the specification in the trigger.
     *
     * @param trigger        subscription trigger including filter information.
     * @param jsonEventData  incoming data in JsonNode format.
     * @return pair of evaluation result of the filter application and the resulting filtered JsonNode.
     */
    private static ImmutablePair<EvaluationResult, JsonNode> applyFilter(SubscriptionTrigger trigger, JsonNode jsonEventData) {
        if (trigger == null) {
            log.debug("Trigger for subscription is null, returning:\n {}", EvaluationResult.empty());
            return new ImmutablePair<>(EvaluationResult.empty(), jsonEventData);
        }

        var selectionResult = applySelectionFilter(trigger.getSelectionFilter(), trigger.getAdvancedSelectionFilter(), jsonEventData);
        if (selectionResult.isMatch()) {
            return new ImmutablePair<>(selectionResult, applyResponseFilter(trigger.getResponseFilter(),trigger.getResponseFilterMode() ,jsonEventData));
        }

        return new ImmutablePair<>(selectionResult, null);
    }

    /**
     * Apply a selection filter or an advanced selection filter to the provided JSON event data.
     *
     * @param selectionFilter            basic selection filter in Map format.
     * @param advancedSelectionFilter    advanced selection filter in Operator format.
     * @param jsonEventData              incoming data in JsonNode format.
     * @return evaluation result of the filter application.
     */
    private static EvaluationResult applySelectionFilter(Map<String, String> selectionFilter, Operator advancedSelectionFilter, JsonNode jsonEventData) {
        if (advancedSelectionFilter != null) {
            log.debug("Advanced selection filter is set, applying:\n {}", advancedSelectionFilter);
            return advancedSelectionFilter.evaluate(jsonEventData.toString());
        }

        if (selectionFilter != null) {
            log.debug("Regular selection filter is set, applying:\n {}", selectionFilter);

            for (Map.Entry<String, String> filterEntry : selectionFilter.entrySet()) {
                Optional<String> value = getStringForJsonPath(jsonEventData, filterEntry.getKey());

                if (value.isEmpty()) {
                    log.debug("Value at specified path not found, returning error.");
                    return EvaluationResult.withError(ComparisonOperator.instantiate(ComparisonOperatorEnum.EQ, filterEntry.getKey(), filterEntry.getValue()), "<Legacy> Could not find value at specified path.");
                }

                if (!Objects.equals(filterEntry.getValue(), value.get())) {
                    log.debug("Value at specified path does not match expected value, returning error.");
                    return EvaluationResult.withError(ComparisonOperator.instantiate(ComparisonOperatorEnum.EQ, filterEntry.getKey(), filterEntry.getValue()), "<Legacy> Value at specified path does not match expected value.");
                }

            }
        }

        log.debug("Neither advanced nor regular selection filter is set, returning valid result.");
        return EvaluationResult.valid(ComparisonOperator.instantiate(ComparisonOperatorEnum.EQ, "", ""));

    }

    /**
     * Apply a response filter to the provided JSON event data.
     * Creates a new object only containing the data of the response filter keys.
     * <br><br>
     * For example, if a responseFilter is "user.name" and the JSON structure is <code>{"user": {"name": "John", "age": 10}}</code>,
     * the response becomes <code>{"user": {"name": "John"}}</code>.
     * @param responseFilter   response filter in List format.
     * @param jsonEventData    incoming data in JsonNode format.
     * @return filtered JsonNode after response filter is applied.
     */
    private static JsonNode applyResponseFilter(List<String> responseFilter, SubscriptionTrigger.ResponseFilterMode mode, JsonNode jsonEventData) {
        // what fields should be sent to consumer
        log.debug("Applying response filter {} with mode {}.", responseFilter, mode);
        if (responseFilter != null && !responseFilter.isEmpty()) {

            mode = Optional.ofNullable(mode).orElse(SubscriptionTrigger.ResponseFilterMode.INCLUDE);
            var strippedNode = objectMapper.createObjectNode();

            if (mode == SubscriptionTrigger.ResponseFilterMode.EXCLUDE) {
                strippedNode = jsonEventData.deepCopy();
            }

            for (String filter : responseFilter) {
                String path = '/' + filter.replace('.', '/');
                JsonNode node = jsonEventData.at(path);

                if (!node.isMissingNode()) {
                    var p = path.substring(1);
                    String[] pathToken = p.split("/");

                    ObjectNode nodeAtPath = getNodeAtPath(strippedNode, pathToken);
                    var key = pathToken[pathToken.length - 1];
                    if (mode == SubscriptionTrigger.ResponseFilterMode.EXCLUDE) {
                        nodeAtPath.remove(key);
                    } else if (mode == SubscriptionTrigger.ResponseFilterMode.INCLUDE) {
                        nodeAtPath.set(key, node);
                    }
                }
            }

            return strippedNode;
        }
        return jsonEventData;
    }

    private static ObjectNode getNodeAtPath(ObjectNode strippedNode, String[] pathToken) {
        ObjectNode nodeAtPath = strippedNode;
        for (var i = 0; i < pathToken.length - 1; i++) {
            var pathTokenI = pathToken[i];
            var child = nodeAtPath.get(pathTokenI);
            if (child == null || child.isNull()) {
                nodeAtPath = nodeAtPath.putObject(pathTokenI);
            } else if (child.isObject()) {
                nodeAtPath = (ObjectNode) child;
            }
        }
        return nodeAtPath;
    }

    /**
     * Get a string from a JsonNode object based on a provided JSON path.
     *
     * @param object   json object in JsonNode format from where string is to extracted
     * @param path     JSON path where the requested string is located
     * @return requested string in an Optional format
     */
    private static Optional<String> getStringForJsonPath(JsonNode object, String path) {
        if (object == null || path == null || path.isEmpty()) {
            return Optional.empty();
        }

        path = '/' + path.replace('.', '/');
        JsonNode node = object.at(path);

        if (node.isMissingNode()) {
            return Optional.empty();
        }

        return Optional.ofNullable(object.at(path).asText());
    }
}
