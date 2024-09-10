package org.finos.orr;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.rosetta.model.lib.RosettaModelObject;
import com.rosetta.model.lib.functions.RosettaFunction;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class OrrUtils {
    
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

    public static Method getEvalMethod(Class<?> functionClass) {
        return Arrays.stream(functionClass.getMethods()).filter(f -> f.getName().equals("evaluate"))
                .filter(x -> x.getParameterCount() == 1)
                .filter(x -> x.getReturnType() != Object.class)
                .findFirst().orElseThrow();
    }
    
    @SuppressWarnings("unchecked")
    public static Class<RosettaModelObject> getFunctionInputType(Class<?> functionClass) {
        Method evalMethod = getEvalMethod(functionClass);
        Parameter parameter = evalMethod.getParameters()[0];
        return (Class<RosettaModelObject>) parameter.getType();
    }

    @SuppressWarnings("unchecked")
    public static Class<RosettaModelObject> getFunctionReturnType(Class<?> functionClass) {
        Method evalMethod = getEvalMethod(functionClass);
        return (Class<RosettaModelObject>) evalMethod.getReturnType();
    }

    @SuppressWarnings("unchecked")
    static Class<RosettaFunction> getFunctionClass(String functionName) throws ClassNotFoundException {
        return (Class<RosettaFunction>) Class.forName(functionName);
    }

}
