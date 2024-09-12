package org.finos.orr;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.regnosys.drr.DrrRuntimeModule;
import com.regnosys.rosetta.common.serialisation.RosettaObjectMapperCreator;
import com.rosetta.model.lib.functions.RosettaFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.unsafe.types.VariantVal;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createMapType;

/**
 * Spark application to run a Rosetta projection function on a Spark table.
 * The application takes 5 arguments:
 *  - output-table: The name of the output table to write the results to.
 *  - input-table: The name of the input table to read the data from.
 *  - function-name: The fully qualified name of the Rosetta function to run.
 *  - xml-config-path: The path to the XML configuration file.
 *  - catalog.schema: The name of the database/schema to use.
 *
 *  For example:
 *  ["esma_emir_trade_iso20022",
 *  "esma_emir_trade_report",
 *  "drr.projection.iso20022.esma.emir.refit.trade.functions.Project_EsmaEmirTradeReportToIso20022",
 *  "xml-config/auth030-esma-rosetta-xml-config.json",
 *  "opensource_reg_reporting.orr"]
 */
public class SparkProjectRunner {

    public static final ObjectMapper OBJECT_MAPPER = RosettaObjectMapperCreator.forJSON().create();

    /**
     * Main method for the Spark application.
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        if (args.length != 5) {
            printUsage();
            return;
        }
        // Read command line arguments
        String outputTable = args[0];
        String inputTable = args[1];
        String functionName = args[2];
        String xmlConfigPath = args[3];
        String databaseName = args[4];

        SparkSession spark = SparkSession.builder().appName(outputTable).getOrCreate();

        // Register the UDF
        UDF1<VariantVal, Map<String, String>> runProject = jsonInput -> runProject(jsonInput.toString(), functionName, xmlConfigPath);
        spark.udf().register(outputTable, runProject, createMapType(StringType, StringType));

        // Read the input table
        Dataset<Row> df = spark.read().table("%s.%s".formatted(databaseName, inputTable)).select(
                        col("identifier"), col("name"), col("data"))
                // Call the UDF
                .withColumn("results", callUDF(outputTable, col("data")))
                // Extract the results
                .drop(col("data"))
                .withColumn("data", parse_json(col("results.data")))
                .withColumn("xml", col("results.xml"))
                .drop(col("results"));

        // Write the results to the output table
        df.write().mode(SaveMode.Overwrite).option("mergeSchema", "true").saveAsTable("%s.%s".formatted(databaseName, outputTable));
    }

    /**
     * Prints the usage instructions.
     */
    private static void printUsage() {
        System.out.println("Usage: <output-table> <input-table> <function-name> <xml-config-path> <catalog.schema>");
        System.out.println("""
                Example: ["esma_emir_trade_iso20022",
                           "esma_emir_trade_report",
                          "drr.projection.iso20022.esma.emir.refit.trade.functions.Project_EsmaEmirTradeReportToIso20022",
                           "xml-config/auth030-esma-rosetta-xml-config.json",
                          "opensource_reg_reporting.orr"]
                """);
    }

    /**
     * Runs the Rosetta projection function on the given JSON input.
     * @param jsonInput The JSON input to the function.
     * @param functionName The fully qualified name of the Rosetta function to run.
     * @param xmlConfigPath The path to the XML configuration file.
     * @return A map containing the XML and JSON output of the function.
     * @throws IOException If an error occurs while reading the input or writing the output.
     * @throws ClassNotFoundException If the function class cannot be found.
     * @throws InvocationTargetException If an error occurs while invoking the function.
     * @throws IllegalAccessException If the function cannot be accessed.
     */
    public static Map<String, String> runProject(String jsonInput, String functionName, String xmlConfigPath) throws IOException, ClassNotFoundException, InvocationTargetException, IllegalAccessException {
        // Create a Guice injector
        Injector injector = Guice.createInjector(new DrrRuntimeModule());
        // Get the function class
        Class<RosettaFunction> functionClass = OrrUtils.getFunctionClass(functionName);
        // Get the evaluate method
        Method evalulateMethod = OrrUtils.getEvalMethod(functionClass);
        // Get the function input type
        Class<?> functionInputTypeClass = OrrUtils.getFunctionInputType(functionClass);
        // Get an instance of the function
        Object projectFunction = injector.getInstance(functionClass);

        // Deserialize the JSON input
        Object transactionReportInstruction = OBJECT_MAPPER.readValue(jsonInput, functionInputTypeClass);
        // Invoke the function
        Object evaluate = evalulateMethod.invoke(projectFunction, transactionReportInstruction);
        // Serialize the output to XML
        InputStream conf = SparkProjectRunner.class.getClassLoader().getResourceAsStream(xmlConfigPath);
        ObjectMapper xmlMapper = RosettaObjectMapperCreator.forXML(conf).create();
        String xml = xmlMapper.writeValueAsString(evaluate);
        // Serialize the output to JSON
        String data = OBJECT_MAPPER.writeValueAsString(evaluate);
        // Create a map to store the results
        HashMap<String, String> result = new HashMap<>();
        result.put("xml", xml);
        result.put("data", data);
        return result;
    }
}