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

/**
 * Utility class for the ORR project.
 */
public class OrrUtils {

    /**
     * Flattens a JSON object into a map of key-value pairs.
     * For nested objects, the key is the path to the value, separated by dots.
     * For array elements, the key is the array index.
     *
     * @param jsonNode The JSON object to flatten.
     * @return A map containing the flattened key-value pairs.
     */
    public static Map<String, String> flattenJson(JsonNode jsonNode) {
        Map<String, String> result = new HashMap<>();
        flattenJsonRecursive(jsonNode, "", result);
        return result;
    }

    /**
     * Recursive helper function for flattening a JSON object.
     *
     * @param jsonNode     The JSON object to flatten.
     * @param currentPath The current path to the value being processed.
     * @param result      The map to store the flattened key-value pairs.
     */
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

    /**
     * Gets the "evaluate" method from a Rosetta function class.
     * This assumes there is only one "evaluate" method with a single parameter and a non-Object return type.
     *
     * @param functionClass The Rosetta function class.
     * @return The "evaluate" method.
     */
    public static Method getEvalMethod(Class<?> functionClass) {
        return Arrays.stream(functionClass.getMethods()).filter(f -> f.getName().equals("evaluate"))
                .filter(x -> x.getParameterCount() == 1)
                .filter(x -> x.getReturnType() != Object.class)
                .findFirst().orElseThrow();
    }

    /**
     * Gets the input type of a Rosetta function.
     *
     * @param functionClass The Rosetta function class.
     * @return The input type of the function.
     */
    @SuppressWarnings("unchecked")
    public static Class<RosettaModelObject> getFunctionInputType(Class<?> functionClass) {
        Method evalMethod = getEvalMethod(functionClass);
        Parameter parameter = evalMethod.getParameters()[0];
        return (Class<RosettaModelObject>) parameter.getType();
    }

    /**
     * Gets the return type of a Rosetta function.
     *
     * @param functionClass The Rosetta function class.
     * @return The return type of the function.
     */
    @SuppressWarnings("unchecked")
    public static Class<RosettaModelObject> getFunctionReturnType(Class<?> functionClass) {
        Method evalMethod = getEvalMethod(functionClass);
        return (Class<RosettaModelObject>) evalMethod.getReturnType();
    }

    /**
     * Gets the class object for a Rosetta function given its fully qualified name.
     *
     * @param functionName The fully qualified name of the Rosetta function.
     * @return The class object for the function.
     * @throws ClassNotFoundException If the class cannot be found.
     */
    @SuppressWarnings("unchecked")
    static Class<RosettaFunction> getFunctionClass(String functionName) throws ClassNotFoundException {
        return (Class<RosettaFunction>) Class.forName(functionName);
    }
}