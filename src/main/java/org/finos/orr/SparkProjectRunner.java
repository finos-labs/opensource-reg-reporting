package org.finos.orr;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.regnosys.drr.DrrRuntimeModule;
import com.regnosys.rosetta.common.serialisation.RosettaObjectMapperCreator;
import com.rosetta.model.lib.functions.RosettaFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.VariantVal;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createMapType;

public class SparkProjectRunner {

    public static final ObjectMapper OBJECT_MAPPER = RosettaObjectMapperCreator.forJSON().create();

    public static void main(String[] args) {
        if (args.length != 5) {
            printUsage();
            return;
        }
        String outputTable = args[0];
        String inputTable = args[1];
        String functionName = args[2];
        String xmlConfigPath = args[3];
        String databaseName = args[4];

        SparkSession spark = SparkSession.builder().appName(outputTable).getOrCreate();

        UDF1<VariantVal, Map<String, String>> runProject = jsonInput -> runProject(jsonInput.toString(), functionName, xmlConfigPath);

        spark.udf().register(outputTable, runProject, createMapType(StringType, StringType));

        Dataset<Row> df = spark.read().table("%s.%s".formatted(databaseName, inputTable)).select(
                        col("identifier"), col("name"), col("data"))
                .withColumn("results", callUDF(outputTable, col("data")))
                .drop(col("data"))
                .withColumn("data", col("results.data"))
                .withColumn("xml", col("results.xml"))
                .drop(col("results"));

        df.write().mode(SaveMode.Overwrite).option("mergeSchema", "true").saveAsTable("%s.%s".formatted(databaseName, outputTable));
    }

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

    public static Map<String, String> runProject(String jsonInput, String functionName, String xmlConfigPath) throws IOException, ClassNotFoundException, InvocationTargetException, IllegalAccessException {
        Injector injector = Guice.createInjector(new DrrRuntimeModule());
        Class<RosettaFunction> functionClass = OrrUtils.getFunctionClass(functionName);
        Method evalulateMethod = OrrUtils.getEvalMethod(functionClass);
        Class<?> functionInputTypeClass = OrrUtils.getFunctionInputType(functionClass);
        Object projectFunction = injector.getInstance(functionClass);

        Object transactionReportInstruction = OBJECT_MAPPER.readValue(jsonInput, functionInputTypeClass);
        Object evaluate = evalulateMethod.invoke(projectFunction, transactionReportInstruction);
        InputStream conf = SparkProjectRunner.class.getClassLoader().getResourceAsStream(xmlConfigPath);
        ObjectMapper xmlMapper = RosettaObjectMapperCreator.forXML(conf).create();
        String xml = xmlMapper.writeValueAsString(evaluate);
        String data = OBJECT_MAPPER.writeValueAsString(evaluate);
        HashMap<String, String> result = new HashMap<>();
        result.put("xml", xml);
        result.put("data", data);
        return result;
    }
}