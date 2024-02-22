// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionTrigger;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FiltersTest {
    @Test
    void includeFilterShouldOnlyIncludeFields() throws URISyntaxException, IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        JsonNode payload = new ObjectMapper().readTree(new File(getClass().getClassLoader().getResource("response-filter-test-data/payload.json").toURI()));
        Method method = Filters.class.getDeclaredMethod("applyResponseFilter", List.class, SubscriptionTrigger.ResponseFilterMode.class, JsonNode.class);
        method.setAccessible(true);

        JsonNode response = (JsonNode) method.invoke(null, List.of("customer.number", "total"), SubscriptionTrigger.ResponseFilterMode.INCLUDE, payload);
        JsonNode expectedOutput = new ObjectMapper().readTree(new File(getClass().getClassLoader().getResource("response-filter-test-data/payload-include-expected-output.json").toURI()));

        assertEquals(expectedOutput, response);

    }

    @Test
    void excludeFilterShouldOnlyExcludeFields() throws URISyntaxException, IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        JsonNode payload = new ObjectMapper().readTree(new File(getClass().getClassLoader().getResource("response-filter-test-data/payload.json").toURI()));
        Method method = Filters.class.getDeclaredMethod("applyResponseFilter", List.class, SubscriptionTrigger.ResponseFilterMode.class, JsonNode.class);
        method.setAccessible(true);

        JsonNode response = (JsonNode) method.invoke(null, List.of("customer.number", "total"), SubscriptionTrigger.ResponseFilterMode.EXCLUDE, payload);
        JsonNode expectedOutput = new ObjectMapper().readTree(new File(getClass().getClassLoader().getResource("response-filter-test-data/payload-exclude-expected-output.json").toURI()));

        assertEquals(expectedOutput, response);
    }

    @Test
    void noFilterModeShouldIncludeFields() throws URISyntaxException, IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        JsonNode payload = new ObjectMapper().readTree(new File(getClass().getClassLoader().getResource("response-filter-test-data/payload.json").toURI()));
        Method method = Filters.class.getDeclaredMethod("applyResponseFilter", List.class, SubscriptionTrigger.ResponseFilterMode.class, JsonNode.class);
        method.setAccessible(true);

        JsonNode response = (JsonNode) method.invoke(null, List.of("customer.number", "total"), null, payload);
        JsonNode expectedOutput = new ObjectMapper().readTree(new File(getClass().getClassLoader().getResource("response-filter-test-data/payload-include-expected-output.json").toURI()));

        assertEquals(expectedOutput, response);
    }
}