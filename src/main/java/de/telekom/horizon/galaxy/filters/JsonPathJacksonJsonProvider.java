// Copyright 2026 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.filters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.JsonPathException;
import com.jayway.jsonpath.spi.json.AbstractJsonProvider;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * JsonPathJacksonJsonProvider is a custom implementation of the AbstractJsonProvider
 * that uses Jackson to parse and generate JSON. This implementation is based on the <a href="https://github.com/json-path/JsonPath/blob/b6c60b3deef74a83eaa92c8dca7d0bc097e957cd/json-path/src/main/java/com/jayway/jsonpath/spi/json/JacksonJsonNodeJsonProvider.java">JacksonJsonNodeJsonProvider</a>.
 */
public class JsonPathJacksonJsonProvider extends AbstractJsonProvider {

    private final ObjectMapper objectMapper;

    public JsonPathJacksonJsonProvider() {
        this(new ObjectMapper());
    }

    public JsonPathJacksonJsonProvider(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Object parse(String json) throws InvalidJsonException {
        try {
            return objectMapper.readTree(json);
        } catch (IOException e) {
            throw new InvalidJsonException(e, json);
        }
    }

    @Override
    public Object parse(byte[] json)
            throws InvalidJsonException {
        try {
            return objectMapper.readTree(json);
        } catch (IOException e) {
            throw new InvalidJsonException(e, new String(json, StandardCharsets.UTF_8));
        }
    }

    @Override
    public Object parse(InputStream jsonStream, String charset) throws InvalidJsonException {
        try {
            return objectMapper.readTree(new InputStreamReader(jsonStream, charset));
        } catch (IOException e) {
            throw new InvalidJsonException(e);
        }
    }

    @Override
    public String toJson(Object obj) {
        if (!(obj instanceof JsonNode)) {
            throw new JsonPathException("Not a JSON Node");
        }
        return obj.toString();
    }

    @Override
    public Object createArray() {
        return new ArrayList<>();
    }

    @Override
    public Object createMap() {
        return JsonNodeFactory.instance.objectNode();
    }

    public Object unwrap(Object o) {
        if (o == null) {
            return null;
        }
        if (!(o instanceof JsonNode)) {
            return o;
        }

        JsonNode e = (JsonNode) o;

        if (e.isValueNode()) {

            if (e.isTextual()) {
                return e.asText();
            } else if (e.isBoolean()) {
                return e.asBoolean();
            } else if (e.isInt()) {
                return e.asInt();
            } else if (e.isLong()) {
                return e.asLong();
            } else if (e.isBigInteger()) {
                return e.bigIntegerValue();
            } else if (e.isDouble()) {
                return e.doubleValue();
            } else if (e.isFloat()) {
                return e.floatValue();
            } else if (e.isBigDecimal()) {
                return e.decimalValue();
            } else if (e.isNull()) {
                return null;
            }
        }
        return o;
    }


    @Override
    public boolean isArray(Object obj) {
        return (obj instanceof ArrayNode || obj instanceof List);
    }

    @Override
    public Object getArrayIndex(Object obj, int idx) {
        if (obj instanceof List) {
            return ((List<?>) obj).get(idx);
        }
        return toJsonArray(obj).get(idx);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setArrayIndex(Object array, int index, Object newValue) {
        if (!isArray(array)) {
            throw new UnsupportedOperationException();
        } else if (array instanceof List) {
            List<Object> list = (List<Object>) array;
            if (index == list.size()) {
                list.add(newValue);
            } else {
                list.set(index, newValue);
            }
        } else {
            ArrayNode arrayNode = toJsonArray(array);
            if (index == arrayNode.size()){
                arrayNode.add(createJsonElement(newValue));
            }else {
                arrayNode.set(index, createJsonElement(newValue));
            }
        }
    }

    @Override
    public Object getMapValue(Object obj, String key) {
        ObjectNode jsonObject = toJsonObject(obj);
        Object o = jsonObject.get(key);
        if (!jsonObject.has(key)) {
            return UNDEFINED;
        } else {
            return o;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setProperty(Object obj, Object key, Object value) {
        if (isMap(obj)) {
            setValueInObjectNode((ObjectNode) obj, key, value);
        } else if (obj instanceof List) {
            List<Object> list = (List<Object>) obj;
            int index;
            if (key != null) {
                index = key instanceof Integer ? (Integer) key : Integer.parseInt(key.toString());
            } else {
                index = list.size();
            }
            if (index == list.size()) {
                list.add(value);
            } else {
                list.set(index, value);
            }
        } else {
            ArrayNode array = toJsonArray(obj);
            int index;
            if (key != null) {
                index = key instanceof Integer ? (Integer) key : Integer.parseInt(key.toString());
            } else {
                index = array.size();
            }
            if (index == array.size()) {
                array.add(createJsonElement(value));
            } else {
                array.set(index, createJsonElement(value));
            }
        }
    }

    public void removeProperty(Object obj, Object key) {
        if (isMap(obj)) {
            toJsonObject(obj).remove(key.toString());
        } else if (obj instanceof List) {
            List<?> list = (List<?>) obj;
            int index = key instanceof Integer ? (Integer) key : Integer.parseInt(key.toString());
            list.remove(index);
        } else {
            ArrayNode array = toJsonArray(obj);
            int index = key instanceof Integer ? (Integer) key : Integer.parseInt(key.toString());
            array.remove(index);
        }
    }

    @Override
    public boolean isMap(Object obj) {
        return (obj instanceof ObjectNode);
    }

    @Override
    public Collection<String> getPropertyKeys(Object obj) {
        List<String> keys = new ArrayList<>();

        Iterator<String> iter = toJsonObject(obj).fieldNames();
        while (iter.hasNext()){
            keys.add(iter.next());
        }
        return keys;
    }

    @Override
    public int length(Object obj) {
        if (obj instanceof List) {
            return ((List<?>) obj).size();
        } else if (obj instanceof ArrayNode) {
            return ((ArrayNode) obj).size();
        } else if (isMap(obj)) {
            return toJsonObject(obj).size();
        } else {
            if (obj instanceof TextNode element) {
                return element.size();
            }
        }
        throw new JsonPathException("length operation can not applied to " + (obj != null ? obj.getClass().getName()
                : "null"));
    }

    @Override
    public Iterable<?> toIterable(Object obj) {
        if (obj instanceof List) {
            return (List<?>) obj;
        }

        ArrayNode arr = toJsonArray(obj);
        Iterator<?> iterator = arr.iterator();
        return (Iterable<Object>) () -> new Iterator<>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Object next() {
                return unwrap(iterator.next());
            }
        };
    }

    private JsonNode createJsonElement(Object o) {
        if (o != null) {
            if (o instanceof JsonNode) {
                return (JsonNode) o;
            } else {
                return objectMapper.valueToTree(o);
            }
        } else {
            return null;
        }
    }

    private ArrayNode toJsonArray(Object o) {
        if (o instanceof ArrayNode) {
            return (ArrayNode) o;
        } else if (o instanceof List) {
            return objectMapper.valueToTree(o);
        }
        throw new JsonPathException("Cannot convert " + (o != null ? o.getClass().getName() : "null") + " to ArrayNode");
    }

    private ObjectNode toJsonObject(Object o) {
        return (ObjectNode) o;
    }

    private void setValueInObjectNode(ObjectNode objectNode, Object key, Object value) {
        // jlolling: necessary to avoid deprecated methods and to avoid creating a cloned node. Bug: #211
        if (value instanceof JsonNode) {
            objectNode.set(key.toString(), (JsonNode) value);
        } else if (value instanceof String) {
            objectNode.put(key.toString(), (String) value);
        } else if (value instanceof Integer) {
            objectNode.put(key.toString(), (Integer) value);
        } else if (value instanceof Long) {
            objectNode.put(key.toString(), (Long) value);
        } else if (value instanceof Short) {
            objectNode.put(key.toString(), (Short) value);
        } else if (value instanceof BigInteger) {
            objectNode.put(key.toString(), (BigInteger) value);
        } else if (value instanceof Double) {
            objectNode.put(key.toString(), (Double) value);
        } else if (value instanceof Float) {
            objectNode.put(key.toString(), (Float) value);
        } else if (value instanceof BigDecimal) {
            objectNode.put(key.toString(), (BigDecimal) value);
        } else if (value instanceof Boolean) {
            objectNode.put(key.toString(), (Boolean) value);
        } else if (value instanceof byte[]) {
            objectNode.put(key.toString(), (byte[]) value);
        } else if (value == null) {
            objectNode.set(key.toString(), null); // this will create a null-node
        } else {
            objectNode.set(key.toString(), createJsonElement(value));
        }
    }

}
