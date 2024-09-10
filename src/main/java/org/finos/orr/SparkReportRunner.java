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

public class SparkReportRunner {

    public static final ObjectMapper OBJECT_MAPPER = RosettaObjectMapperCreator.forJSON().create();

    public static void main(String[] args) {
        if (args.length != 4) {
            printUsage();
            return;
        }
        String outputTable = args[0];
        String inputTable = args[1];
        String functionName = args[2];
        String databaseName = args[3];

        SparkSession spark = SparkSession.builder().appName(outputTable).getOrCreate();

        UDF1<VariantVal, Map<String, String>> runReport = jsonInput -> runReport(jsonInput.toString(), functionName);
        spark.udf().register(outputTable, runReport, createMapType(StringType, StringType));

        Dataset<Row> df = spark.read().table("%s.%s".formatted(databaseName, inputTable)).select(
                        col("identifier"), col("name"), col("data"))
                .withColumn("result", callUDF(outputTable, col("data")))
                .withColumn("data", parse_json(col("result.object")));

        List<String> explodedColNames = getExplodedColNames(df, "result", "object");

        for (String colName : explodedColNames) {
            df = df.withColumn(colName, col("result.`" + colName + "`"));
        }
        df = df.drop(col("result"));

        df.write().mode(SaveMode.Overwrite).option("mergeSchema", "true").saveAsTable("%s.%s".formatted(databaseName, outputTable));

    }

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

    private static void printUsage() {
        System.out.println("Usage: <output-table> <input-table> <function-name> <catalog.schema>");
        System.out.println("""
                Example: ["cftc_part_43",
                          "reportable_event",
                          "drr.regulation.cftc.rewrite.reports.CFTCPart45ReportFunction",
                          "opensource_reg_reporting.orr"]
                """);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static HashMap<String, String> runReport(String jsonInput, String functionName) throws JsonProcessingException, ClassNotFoundException {
        Injector injector = Guice.createInjector(new DrrRuntimeModule());
        Class<?> functionClass = OrrUtils.getFunctionClass(functionName);
        Class<RosettaModelObject> functionInputType = OrrUtils.getFunctionInputType(functionClass);
        ReportFunction reportFunction = (ReportFunction<?, ?>) injector.getInstance(functionClass);
        Object transactionReportInstruction = OBJECT_MAPPER.readValue(jsonInput, functionInputType);
        Object evaluate = reportFunction.evaluate(transactionReportInstruction);
        HashMap<String, String> result = new HashMap<>();
        String object = OBJECT_MAPPER.writeValueAsString(evaluate);
        result.put("object", object);
        JsonNode jsonNode = OBJECT_MAPPER.readTree(object);
        result.putAll(OrrUtils.flattenJson(jsonNode));
        return result;
    }
}