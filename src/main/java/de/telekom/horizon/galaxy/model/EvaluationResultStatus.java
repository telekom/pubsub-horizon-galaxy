// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.model;

public enum EvaluationResultStatus {
    SCOPE_FILTER_ERROR,
    CONSUMER_FILTER_ERROR,
    INVALID_PAYLOAD_ERROR,
    NO_FILTER,
    MATCH
}
