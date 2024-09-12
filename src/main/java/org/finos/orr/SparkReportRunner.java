package org.finos.orr;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.regnosys.drr.DrrRuntimeModule;
import com.regnosys.rosetta.common.serialisation.RosettaObjectMapperCreator;
import com.rosetta.model.lib.RosettaModelObject;
import com.rosetta.model.lib.reports.ReportFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.unsafe.types.VariantVal;

import java.util.*;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createMapType;

/**
 * Spark application to run a Rosetta report function on a Spark table.
 * The application takes 4 arguments:
 *  - output-table: The name of the output table to write the results to.
 *  - input-table: The name of the input table to read the data from.
 *  - function-name: The fully qualified name of the Rosetta function to run.
 *  - catalog.schema: The name of the database/schema to use.
 *
 *  For example:
 *  ["cftc_part_43",
 *  "reportable_event",
 *  "drr.regulation.cftc.rewrite.reports.CFTCPart45ReportFunction",
 *  "opensource_reg_reporting.orr"]
 */
public class SparkReportRunner {

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
        UDF1<VariantVal, Map<String, String>> runReport = jsonInput -> runReport(jsonInput.toString(), functionName);
        spark.udf().register(outputTable, runReport, createMapType(StringType, StringType));

        // Read the input table
        Dataset<Row> df = spark.read().table("%s.%s".formatted(databaseName, inputTable)).select(
                        col("identifier"), col("name"), col("data"))
                // Call the UDF
                .withColumn("result", callUDF(outputTable, col("data")))
                // Extract the results
                .withColumn("data", parse_json(col("result.object")));

        // Explode the result map into separate columns
        List<String> explodedColNames = getExplodedColNames(df, "result", "object");
        for (String colName : explodedColNames) {
            df = df.withColumn(colName, col("result.`" + colName + "`"));
        }
        df = df.drop(col("result"));

        // Write the results to the output table
        df.write().mode(SaveMode.Overwrite).option("mergeSchema", "true").saveAsTable("%s.%s".formatted(databaseName, outputTable));

    }

    /**
     * Gets the names of the columns to be exploded from the result map.
     * @param df The DataFrame containing the result map.
     * @param resultCol The name of the column containing the result map.
     * @param resultObjectCol The name of the column containing the result object.
     * @return A list of column names to be exploded.
     */
    private static List<String> getExplodedColNames(Dataset<Row> df, String resultCol, String resultObjectCol) {
        return df
                .select(explode(map_keys(col(resultCol))))
                .drop(resultCol)
                .distinct()
                .sort()
                .collectAsList()
                .stream()
                .map(Row::mkString)
                .filter(x -> !x.equals(resultObjectCol))
                .sorted()
                .toList();
    }

    /**
     * Prints the usage instructions.
     */
    private static void printUsage() {
        System.out.println("Usage: <output-table> <input-table> <function-name> <catalog.schema>");
        System.out.println("""
                Example: ["cftc_part_43",
                          "reportable_event",
                          "drr.regulation.cftc.rewrite.reports.CFTCPart45ReportFunction",
                          "opensource_reg_reporting.orr"]
                """);
    }

    /**
     * Runs the Rosetta report function on the given JSON input.
     * @param jsonInput The JSON input to the function.
     * @param functionName The fully qualified name of the Rosetta function to run.
     * @return A map containing the report output object and flattened key-value pairs.
     * @throws JsonProcessingException If an error occurs while processing JSON.
     * @throws ClassNotFoundException If the function class cannot be found.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static HashMap<String, String> runReport(String jsonInput, String functionName) throws JsonProcessingException, ClassNotFoundException {
        // Create a Guice injector
        Injector injector = Guice.createInjector(new DrrRuntimeModule());
        // Get the function class
        Class<?> functionClass = OrrUtils.getFunctionClass(functionName);
        // Get the function input type
        Class<RosettaModelObject> functionInputType = OrrUtils.getFunctionInputType(functionClass);
        // Get an instance of the function
        ReportFunction reportFunction = (ReportFunction<?, ?>) injector.getInstance(functionClass);
        // Deserialize the JSON input
        Object transactionReportInstruction = OBJECT_MAPPER.readValue(jsonInput, functionInputType);
        // Invoke the function
        Object evaluate = reportFunction.evaluate(transactionReportInstruction);
        // Create a map to store the results
        HashMap<String, String> result = new HashMap<>();
        // Serialize the output to JSON and store it
        String object = OBJECT_MAPPER.writeValueAsString(evaluate);
        result.put("object", object);
        // Flatten the JSON output and add it to the result map
        JsonNode jsonNode = OBJECT_MAPPER.readTree(object);
        result.putAll(OrrUtils.flattenJson(jsonNode));
        // Return the result map
        return result;
    }
}