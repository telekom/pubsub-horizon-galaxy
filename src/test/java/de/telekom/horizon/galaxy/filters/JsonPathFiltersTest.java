// Copyright 2026 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.filters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionTrigger.ResponseFilterMode;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JsonPathFiltersTest {

    private static ObjectMapper objectMapper;

    @BeforeAll
    public static void setUp() {
        objectMapper = new ObjectMapper();
    }

    @Test
    void applyJsonPathResponseFilter() {
        var jsonString = """
                {
                    "hello": "world",
                    "foo": {
                        "bar": "fizz",
                        "the-cake": "is a lie",
                        "@prefixed": "at-prefixed"
                    },
                    "@prefixed": "foobar",
                    "buzz": [
                        {
                            "value": 42
                        },
                        {
                            "value": 21
                        }
                    ],
                    "numbers": [0, 1, 2, 3, 4, 5]
                }
                """;

        JsonNode jsonNode = null;
        try {
            jsonNode = objectMapper.readTree(jsonString);
        } catch (JsonProcessingException ex) {
            fail(ex);
        }

        // Include
        {
            var includeJsonNode = JsonPathFilters.applyJsonPathResponseFilter(List.of("$.hello", "$.foo.bar", "$.@prefixed", "foo.@prefixed", "$.buzz[?(@.value == 21)]", "$.numbers[?(@ < 3)]"), ResponseFilterMode.INCLUDE, jsonNode.deepCopy());
            assertFalse(includeJsonNode.at("/hello").isMissingNode());
            assertFalse(includeJsonNode.at("/foo/bar").isMissingNode());
            assertTrue(includeJsonNode.at("/foo/the-cake").isMissingNode());
            assertFalse(includeJsonNode.at("/buzz").isMissingNode());
            assertFalse(includeJsonNode.at("/foo/@prefixed").isMissingNode());
            assertFalse(includeJsonNode.at("/@prefixed").isMissingNode());

            var filteredBuzz = Lists.newArrayList(includeJsonNode.at("/buzz").elements());
            assertEquals(1, filteredBuzz.size());
            assertEquals(21, filteredBuzz.getFirst().at("/value").asInt());

            List<Integer> remainingNumbers = new ArrayList<>();
            includeJsonNode.at("/numbers").elements().forEachRemaining(v -> remainingNumbers.add(v.asInt()));
            assertEquals(List.of(0,1,2), remainingNumbers);

            assertDoesNotThrow(() -> System.out.println(objectMapper.writeValueAsString(includeJsonNode)));
        }

        // Exclude
        {
            var excludeJsonNode = JsonPathFilters.applyJsonPathResponseFilter(List.of("$.buzz[?(@.value == 21)]", "$.numbers[?(@ < 3)]", "foo.@prefixed"), ResponseFilterMode.EXCLUDE, jsonNode.deepCopy());
            assertTrue(excludeJsonNode.at("/buzz/1").isMissingNode());
            assertFalse(excludeJsonNode.at("/buzz/0").isMissingNode());
            assertTrue(excludeJsonNode.at("/foo/@prefixed").isMissingNode());

            List<Integer> remainingNumbers = new ArrayList<>();
            excludeJsonNode.at("/numbers").elements().forEachRemaining(v -> remainingNumbers.add(v.asInt()));
            assertEquals(List.of(3,4,5), remainingNumbers);

            assertDoesNotThrow(() -> System.out.println(objectMapper.writeValueAsString(excludeJsonNode)));
        }
    }
}