// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.utils;

import com.fasterxml.jackson.databind.JsonNode;
import de.telekom.eni.jfilter.operator.EvaluationResult;
import de.telekom.eni.jfilter.operator.comparison.NotEqualsOperator;
import de.telekom.horizon.galaxy.model.EvaluationResultStatus;
import lombok.Getter;

/**
 * This class is a wrapper for the evaluation results and payload after applying
 * all applicable filters to an incoming event. It contains information about
 * the filtered payload, the specific filters' evaluation results, and the status
 * indicating if the filter evaluations were successful.
 * <br><br>
 * The wrapper provides a range of static utility methods to create instances
 * representing various filter operation statuses, such as successful filtering,
 * failure on consumer filter, failure on scope filter, invalid payload errors,
 * and the absence of filters.
 *
 * @see EvaluationResult
 * @see EvaluationResultStatus
 */
@Getter
public class FilterEventMessageWrapper {

    //The filtered payload that the subscriber receives
    private final JsonNode filteredPayload;

    //The evaluation result of the filters
    private final EvaluationResult evaluationResult;

    //Which evaluation failed: The one for the scope-filter, or the customer-filter.
    private final EvaluationResultStatus evaluationResultStatus;

    public FilterEventMessageWrapper(JsonNode filteredPayload) {
        this.filteredPayload = filteredPayload;
        evaluationResult = null;
        evaluationResultStatus = EvaluationResultStatus.MATCH;
    }

    public FilterEventMessageWrapper(EvaluationResult evaluationResult, EvaluationResultStatus evaluationErrorType) {
        this.evaluationResult = evaluationResult;
        this.evaluationResultStatus = evaluationErrorType;
        filteredPayload = null;
    }

    public static FilterEventMessageWrapper withPayloadInvalidError() {
        return new FilterEventMessageWrapper(EvaluationResult.withError(new NotEqualsOperator<>("", ""), "The payload in the incoming event was no valid JSON. Therefore, the filters could not be applied."), EvaluationResultStatus.INVALID_PAYLOAD_ERROR);
    }

    public static FilterEventMessageWrapper withMatch(JsonNode filteredPayload) {
        return new FilterEventMessageWrapper(filteredPayload);
    }

    public static FilterEventMessageWrapper withConsumerError(EvaluationResult evaluationResult) {
        return new FilterEventMessageWrapper(evaluationResult, EvaluationResultStatus.CONSUMER_FILTER_ERROR);
    }

    public static FilterEventMessageWrapper withScopeError(EvaluationResult evaluationResult) {
        return new FilterEventMessageWrapper(evaluationResult, EvaluationResultStatus.SCOPE_FILTER_ERROR);
    }

    public static FilterEventMessageWrapper withNoFilter() {
        return new FilterEventMessageWrapper(null, EvaluationResultStatus.NO_FILTER);
    }

    public boolean isSuccessful() {
        return evaluationResultStatus.equals(EvaluationResultStatus.NO_FILTER) || evaluationResultStatus.equals(EvaluationResultStatus.MATCH);
    }

}
