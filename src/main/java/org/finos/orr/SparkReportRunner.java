package org.finos.orr;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.regnosys.drr.DrrRuntimeModule;
import com.regnosys.rosetta.common.serialisation.RosettaObjectMapperCreator;
import com.rosetta.model.lib.reports.ReportFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.VariantVal;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

public class SparkReportRunner {

    public static final ObjectMapper OBJECT_MAPPER = RosettaObjectMapperCreator.forJSON().create();

    public static void main(String[] args) {
        if (args.length != 5) {
            printUsage();
            return;
        }
        String runName = args[0];
        String functionName = args[1];
        String functionInputType = args[2];
        String databaseName = args[3];
        String inputTable = args[4];

        SparkSession spark = SparkSession.builder().appName(runName).getOrCreate();

        UDF1<VariantVal, Map<String, String>> runReport = jsonInput -> runReport(jsonInput.toString(), functionInputType, functionName);
        spark.udf().register(runName, runReport, DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));

        Dataset<Row> df = spark.read().table("%s.%s".formatted(databaseName, inputTable))
                .withColumn("result", functions.callUDF(runName, col("data")))
                .withColumn("data", functions.parse_json(col("result.object")))
//                .withColumn("exploded", functions.explode(col("result")))
//                .drop(col("result"))
//                .drop(col("object"))
                ;
        df.write().mode(SaveMode.Overwrite).option("mergeSchema", "true")
                .saveAsTable("%s.%s".formatted(databaseName, runName + "_json"));
    }

    private static void printUsage() {
        System.out.println("Usage: SparkReportRunner <run-name> <function-name> <function-input-type> <catalog.schema> <input-table>");
        System.out.println("""
                Example: ["cftc_part_43",
                          "drr.regulation.cftc.rewrite.reports.CFTCPart45ReportFunction",
                          "drr.regulation.common.TransactionReportInstruction",
                          "opensource_reg_reporting.orr",
                          "reportable_event_json"]
                """);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static HashMap<String, String> runReport(String jsonInput, String functionInputType, String functionName) throws JsonProcessingException, ClassNotFoundException {
        Injector injector = Guice.createInjector(new DrrRuntimeModule());
        ReportFunction reportFunction = (ReportFunction<?, ?>) injector.getInstance(Class.forName(functionName));
        Object transactionReportInstruction = OBJECT_MAPPER.readValue(jsonInput, Class.forName(functionInputType));
        Object evaluate = reportFunction.evaluate(transactionReportInstruction);
        HashMap<String, String> result = new HashMap<>();
        String object = OBJECT_MAPPER.writeValueAsString(evaluate);
        result.put("object", object);
        JsonNode jsonNode = OBJECT_MAPPER.readTree(object);
        result.putAll(JsonFlatten.flattenJson(jsonNode));
        return result;
    }
}