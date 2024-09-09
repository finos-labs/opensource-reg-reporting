package org.finos.orr;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.regnosys.drr.DrrRuntimeModule;
import com.regnosys.rosetta.common.serialisation.RosettaObjectMapperCreator;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.VariantVal;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class SparkProjectRunner {

    public static final ObjectMapper OBJECT_MAPPER = RosettaObjectMapperCreator.forJSON().create();

    public static void main(String[] args) {
        if (args.length != 6) {
            System.out.println("Usage: SparkProjectRunner <run-name> <function-name> <function-input-type> <catalog.schema> <input-table>");
            System.out.println("""
                    Example: ["esma_emir_trade_iso20022",
                              "drr.projection.iso20022.esma.emir.refit.trade.functions.Project_EsmaEmirTradeReportToIso20022",
                              "drr.regulation.esma.emir.refit.trade.ESMAEMIRTransactionReport",
                              "xml-config/auth030-esma-rosetta-xml-config.json",
                              "opensource_reg_reporting.orr",
                              "esma_emir_trade_report_json"]
                    """);
        }
        String runName = args[0];
        String functionName = args[1];
        String functionInputType = args[2];
        String xmlConfigPath = args[3];
        String databaseName = args[4];
        String inputTable = args[5];

        SparkSession spark = SparkSession.builder().appName(runName).getOrCreate();

        UDF1<VariantVal, String> runReport = jsonInput -> runProject(jsonInput.toString(), functionInputType, functionName, xmlConfigPath);
        spark.udf().register(runName, runReport, DataTypes.StringType);

        Dataset<Row> df = spark.sql("select * from %s.%s".formatted(databaseName, inputTable));

        df = df.withColumn("data", functions.callUDF(runName, df.col("data")));
//        df = df.withColumn("data", functions.to_xml(df.col("data")));

        df.write().mode(SaveMode.Overwrite).saveAsTable("%s.%s".formatted(databaseName, runName));

        // see https://docs.databricks.com/en/query/formats/xml.html
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static String runProject(String jsonInput, String functionInputType, String functionName, String xmlConfigPath) throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Injector injector = Guice.createInjector(new DrrRuntimeModule());
        Class<?> functionClass = Class.forName(functionName);
        Class<?> functionInputTypeClass = Class.forName(functionInputType);
        Method evalulateMethod = functionClass.getMethod("evaluate", functionInputTypeClass);
        Object transactionReportInstruction = OBJECT_MAPPER.readValue(jsonInput, functionInputTypeClass);
        Object projectFunction = injector.getInstance(functionClass);
        Object evaluate = evalulateMethod.invoke(projectFunction, transactionReportInstruction);
        InputStream conf = SparkProjectRunner.class.getClassLoader().getResourceAsStream(xmlConfigPath);
        ObjectMapper xmlMapper = RosettaObjectMapperCreator.forXML(conf).create();
        return xmlMapper.writeValueAsString(evaluate);
    }
}