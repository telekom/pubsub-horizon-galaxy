// Copyright 2026 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.filters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.*;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionTrigger.ResponseFilterMode;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Slf4j
public class JsonPathFilters {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static JsonNode applyJsonPathResponseFilter(
            List<String> responseFilter,
            ResponseFilterMode filterMode,
            JsonNode jsonEventData
    ) {
        if (Objects.isNull(responseFilter)) {
            return jsonEventData;
        }

        var parseCtx = JsonPath.using(Configuration.defaultConfiguration()
                .jsonProvider(new JsonPathJacksonJsonProvider())
                .mappingProvider(new JacksonMappingProvider())
                .setOptions(
                        Option.SUPPRESS_EXCEPTIONS,
                        Option.DEFAULT_PATH_LEAF_TO_NULL
                ));

        return switch (filterMode) {
            case INCLUDE -> applyIncludeFilter(parseCtx, responseFilter, jsonEventData);
            case EXCLUDE -> applyExcludeFilter(parseCtx, responseFilter, jsonEventData);
            case null -> {
                log.warn("response filter mode is null, defaulting to INCLUDE");
                yield applyIncludeFilter(parseCtx, responseFilter, jsonEventData);
            }
        };
    }

    private static JsonNode applyIncludeFilter(ParseContext parseCtx, List<String> responseFilter, JsonNode data) {
        var eventCtx = parseCtx.parse(data);
        var filteredCtx = parseCtx.parse(objectMapper.createObjectNode());

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
    }

    private static JsonNode applyExcludeFilter(ParseContext parseCtx, List<String> responseFilter, JsonNode data) {
        var jsonNode = data.deepCopy();
        var filteredCtx = parseCtx.parse(jsonNode);
        responseFilter.forEach(filteredCtx::delete);

        return filteredCtx.json();
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

        List<String> parts = new ArrayList<>();
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
                ctx.set(currentPath.toString(), objectMapper.createObjectNode());
            }
        }
    }
}
