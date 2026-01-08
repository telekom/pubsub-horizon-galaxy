// Copyright 2026 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.filters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionTrigger.ResponseFilterMode;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class JsonPathFilters {

    private static ObjectMapper objectMapper;

    private static Configuration jsonPathConfig;

    public static JsonNode applyJsonPathResponseFilter(
            List<String> responseFilter,
            ResponseFilterMode mode,
            JsonNode jsonEventData
    ) {
        if (Objects.isNull(responseFilter)) {
            return jsonEventData;
        }

        var filterMode = Optional.ofNullable(mode).orElse(ResponseFilterMode.INCLUDE);
        var jsonNode = getObjectMapper().createObjectNode();

        DocumentContext eventCtx;
        DocumentContext filteredCtx;

        switch (filterMode) {

            case INCLUDE:
                eventCtx = JsonPath.using(getJsonPathConfig()).parse(jsonEventData);
                filteredCtx = JsonPath.using(getJsonPathConfig()).parse(jsonNode);

                for (var path : responseFilter) {
                    var pathValue = eventCtx.read(path);
                    if (Objects.nonNull(pathValue)) {
                        String targetPath = getTargetPath(path);
                        ensureParentPathExists(filteredCtx, targetPath);
                        
                        if (isFilterExpression(path)) {
                            filteredCtx = filteredCtx.set(targetPath, pathValue);
                        } else {
                            filteredCtx = filteredCtx.set(path, pathValue);
                        }
                    }
                }

                return filteredCtx.json();

            case EXCLUDE:
                jsonNode = jsonEventData.deepCopy();
                filteredCtx = JsonPath.using(getJsonPathConfig()).parse(jsonNode);
                responseFilter.forEach(filteredCtx::delete);
                return filteredCtx.json();

            default:
                throw new RuntimeException("Unsupported response filter mode " + mode);

        }
    }

    private static ObjectMapper getObjectMapper() {
        if (objectMapper != null) {
            return objectMapper;
        }

        objectMapper = new ObjectMapper();
        return objectMapper;
    }

    private static Configuration getJsonPathConfig() {
        if (jsonPathConfig != null) {
            return jsonPathConfig;
        }

        jsonPathConfig = Configuration.defaultConfiguration()
                .jsonProvider(new JsonPathJacksonJsonProvider())
                .mappingProvider(new JacksonMappingProvider())
                .setOptions(
                        Option.SUPPRESS_EXCEPTIONS,
                        Option.DEFAULT_PATH_LEAF_TO_NULL
                );

        return jsonPathConfig;
    }

    private static boolean isFilterExpression(String path) {
        return path.contains("[?(") || path.contains("[?@");
    }

    /**
     * Returns the target path for a filter expression or the array targeted by a filter expression.
     * @param path the JSON path as is.
     * @return the path to the array targeted by the JSON path or the JSON path itself if it is not a filter expression.
     */
    private static String getTargetPath(String path) {
        int filterStart = path.indexOf("[?(");
        if (filterStart == -1) {
            filterStart = path.indexOf("[?@");
        }
        if (filterStart != -1) {
            return path.substring(0, filterStart);
        }
        return path;
    }

    /**
     * Ensure the given path exists within the given context and create it if it does not.
     * @param ctx the context of the JSON document
     * @param path the path to check and/or create
     */
    private static void ensureParentPathExists(DocumentContext ctx, String path) {
        String pathWithoutRoot = path.startsWith("$.") ? path.substring(2) : path;

        List<String> parts = new java.util.ArrayList<>();
        StringBuilder current = new StringBuilder();
        int bracketDepth = 0;
        
        for (char c : pathWithoutRoot.toCharArray()) {
            if (c == '[') {
                bracketDepth++;
                current.append(c);
            } else if (c == ']') {
                bracketDepth--;
                current.append(c);
            } else if (c == '.' && bracketDepth == 0) {
                if (!current.isEmpty()) {
                    parts.add(current.toString());
                    current = new StringBuilder();
                }
            } else {
                current.append(c);
            }
        }
        if (!current.isEmpty()) {
            parts.add(current.toString());
        }

        StringBuilder currentPath = new StringBuilder("$");
        for (int i = 0; i < parts.size() - 1; i++) {
            String part = parts.get(i);
            if (part.contains("[")) {
                part = part.substring(0, part.indexOf('['));
            }
            currentPath.append(".").append(part);
            
            Object existing = ctx.read(currentPath.toString());
            if (existing == null) {
                ctx.set(currentPath.toString(), getObjectMapper().createObjectNode());
            }
        }
    }
}
