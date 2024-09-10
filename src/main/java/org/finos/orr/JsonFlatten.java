package org.finos.orr;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;

import java.util.HashMap;
import java.util.Map;

public class JsonFlatten {
    
    public static Map<String, String> flattenJson(JsonNode jsonNode) {
        Map<String, String> result = new HashMap<>();
        flattenJsonRecursive(jsonNode, "", result);
        return result;
    }

    private static void flattenJsonRecursive(JsonNode jsonNode, String currentPath, Map<String, String> result) {
        if (jsonNode.getNodeType() == JsonNodeType.OBJECT) {
            jsonNode.fieldNames().forEachRemaining(fieldName ->
                    flattenJsonRecursive(jsonNode.get(fieldName), currentPath + fieldName + ".", result));
        } else if (jsonNode.getNodeType() == JsonNodeType.ARRAY) {
            for (int i = 0; i < jsonNode.size(); i++) {
                flattenJsonRecursive(jsonNode.get(i), currentPath + i + ".", result);
            }
        } else { // Leaf node
            result.put(currentPath.substring(0, currentPath.length() - 1), jsonNode.asText());
        }
    }
}
