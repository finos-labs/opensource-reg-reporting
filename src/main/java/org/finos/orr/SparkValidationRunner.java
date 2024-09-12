package org.finos.orr;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.regnosys.drr.DrrRuntimeModule;
import com.regnosys.rosetta.common.serialisation.RosettaObjectMapperCreator;
import com.regnosys.rosetta.common.validation.RosettaTypeValidator;
import com.regnosys.rosetta.common.validation.ValidationReport;
import com.rosetta.model.lib.RosettaModelObject;
import com.rosetta.model.lib.validation.ValidationResult;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.VariantVal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createMapType;

/**
 * Spark application to run Rosetta validations on a Spark table.
 * The application takes 4 arguments:
 *  - output-table: The name of the output table to write the results to.
 *  - input-table: The name of the input table to read the data from.
 *  - function-name: The fully qualified name of the Rosetta function to run.
 *  - catalog.schema: The name of the database/schema to use.
 *
 *  For example:
 *  ["cftc_part_43_validation",
 *  "cftc_part_43_report",
 *  "drr.regulation.cftc.rewrite.reports.CFTCPart45ReportFunction",
 *  "opensource_reg_reporting.orr"]
 */
public class SparkValidationRunner {
    public static final ObjectMapper OBJECT_MAPPER = RosettaObjectMapperCreator.forJSON().create();

    /**
     * Main method for the Spark application.
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        if (args.length != 4) {
            printUsage();
            return;
        }
        // Read command line arguments
        String outputTable = args[0];
        String inputTable = args[1];
        String functionName = args[2];
        String databaseName = args[3];

        // Create a Spark session
        SparkSession spark = SparkSession.builder().appName(outputTable).getOrCreate();

        // Register the UDF
        UDF1<VariantVal, List<Map<String, String>>> runValidations = (jsonInput) -> runValidations(jsonInput.toString(), functionName);
        spark.udf().register(outputTable, runValidations, DataTypes.createArrayType(createMapType(StringType, StringType)));

        // Read the input table
        Dataset<Row> df = spark.read().table("%s.%s".formatted(databaseName, inputTable)).select(
                        col("identifier"),  col("name"), col("data"))
                // Call the UDF
                .withColumn("validations", callUDF(outputTable, col("data")))
                // Extract the results
                .drop(col("data"))
                .withColumn("validation", explode(col("validations")))
                .drop(col("validations"))
                .withColumn("validation_name", col("validation.validation_name"))
                .withColumn("status", col("validation.status"))
                .withColumn("definition", col("validation.definition"))
                .withColumn("path", col("validation.path"))
                .withColumn("failure", col("validation.failure"))
                .withColumn("validation_type", col("validation.validation_type"))
                .drop(col("validation"));

        // Write the results to the output table
        df.write().mode(SaveMode.Overwrite).option("mergeSchema", "true").saveAsTable("%s.%s".formatted(databaseName, outputTable));
    }

    /**
     * Prints the usage instructions.
     */
    private static void printUsage() {
        System.out.println("Usage: <output-table> <input-table> <function-name> <catalog.schema>");
        System.out.println("""
                Example: ["cftc_part_43_validation",
                          "cftc_part_43_report",
                          "drr.regulation.cftc.rewrite.reports.CFTCPart45ReportFunction",
                          "opensource_reg_reporting.orr"]
                """);
    }

    /**
     * Runs the Rosetta validations on the given JSON input.
     * @param jsonInput The JSON input to the function.
     * @param functionName The fully qualified name of the Rosetta function to run.
     * @return A list of maps containing the validation results.
     * @throws JsonProcessingException If an error occurs while processing JSON.
     * @throws ClassNotFoundException If the function class cannot be found.
     */
    public static List<Map<String, String>> runValidations(String jsonInput, String functionName) throws JsonProcessingException, ClassNotFoundException {
        // Create a Guice injector
        Injector injector = Guice.createInjector(new DrrRuntimeModule());
        // Get the Rosetta type validator
        RosettaTypeValidator rosettaTypeValidator = injector.getInstance(RosettaTypeValidator.class);

        // Get the function return type
        Class<RosettaModelObject> functionReturnType = OrrUtils.getFunctionReturnType(OrrUtils.getFunctionClass(functionName));

        // Deserialize the JSON input
        RosettaModelObject object = OBJECT_MAPPER.readValue(jsonInput, functionReturnType);
        // Run the validations
        ValidationReport validationReport = rosettaTypeValidator.runProcessStep(object.getType(), object);
        // Get the validation results
        List<ValidationResult<?>> validationResults = validationReport.getValidationResults();

        // Create a list to store the validation results
        List<Map<String, String>> res = new ArrayList<>();
        // Iterate over the validation results
        for (ValidationResult<?> validationResult : validationResults) {
            // Create a map to store the validation result
            HashMap<String, String> validations = new HashMap<>();
            // Add the validation result to the map
            validations.put("validation_name", validationResult.getName().replace(object.getType().getSimpleName(), ""));
            validations.put("definition", validationResult.getDefinition());
            validations.put("path", validationResult.getPath().toString());
            validations.put("status", validationResult.isSuccess() ? "PASS" : "FAIL");
            validations.put("failure", validationResult.getFailureReason().orElse("NONE"));
            validations.put("validation_type", validationResult.getValidationType().toString());
            // Add the validation result map to the list
            res.add(validations);
        }

        // Return the list of validation results
        return res;
    }
}