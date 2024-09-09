package org.finos.orr;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.regnosys.drr.DrrRuntimeModule;
import com.regnosys.rosetta.common.serialisation.RosettaObjectMapperCreator;
import com.rosetta.model.lib.reports.ReportFunction;
import drr.regulation.cftc.rewrite.CFTCPart45TransactionReport;
import drr.regulation.common.TransactionReportInstruction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.VariantVal;

import java.util.List;

import static org.apache.spark.sql.functions.col;

public class SparkReportRunner {

    public static final ObjectMapper OBJECT_MAPPER = RosettaObjectMapperCreator.forJSON().create();

    public static void main(String[] args) {
        if (args.length != 5) {
            System.out.println("Usage: SparkReportRunner <run-name> <function-name> <function-input-type> <catalog.schema> <input-table>");
            System.out.println("""
                    Example: ["cftc_part_43",
                              "drr.regulation.cftc.rewrite.reports.CFTCPart45ReportFunction",
                              "drr.regulation.common.TransactionReportInstruction",
                              "opensource_reg_reporting.orr",
                              "reportable_event_json"]
                    """);
        }
        String runName = args[0];
        String functionName = args[1];
        String functionInputType = args[2];
        String databaseName = args[3];
        String inputTable = args[4];

        SparkSession spark = SparkSession.builder().appName(runName).getOrCreate();

        UDF1<VariantVal, String> runReport = jsonInput -> runReport(jsonInput.toString(), functionInputType, functionName);
        spark.udf().register(runName, runReport, DataTypes.StringType);

        spark.udf().register(runName, runReport, DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));
        
        Dataset<Row> df = spark.read().table("%s.%s".formatted(databaseName, inputTable))
                .withColumn("data", functions.callUDF(runName, col("data")))
                .withColumn("data", functions.parse_json(col("data")));
        df.write().mode(SaveMode.Overwrite).saveAsTable("%s.%s".formatted(databaseName, runName + "_json"));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static String runReport(String jsonInput, String functionInputType, String functionName) throws JsonProcessingException, ClassNotFoundException {
        Injector injector = Guice.createInjector(new DrrRuntimeModule());
        Class<?> functionClass = Class.forName(functionName);
        Class<?> functionInputTypeClass = Class.forName(functionInputType);
        ReportFunction reportFunction = (ReportFunction<?, ?>) injector.getInstance(functionClass);
        Object transactionReportInstruction = OBJECT_MAPPER.readValue(jsonInput, functionInputTypeClass);
        Object evaluate = reportFunction.evaluate(transactionReportInstruction);
        return OBJECT_MAPPER.writeValueAsString(evaluate);
    }

    public static String runValidationReport(String jsonInput, String functionInputType, String functionName) throws JsonProcessingException, ClassNotFoundException {
        Injector injector = Guice.createInjector(new DrrRuntimeModule());
        Class<?> functionClass = Class.forName(functionName);
        Class<?> functionInputTypeClass = Class.forName(functionInputType);
        ReportFunction reportFunction = (ReportFunction<?, ?>) injector.getInstance(functionClass);
        Object transactionReportInstruction = OBJECT_MAPPER.readValue(jsonInput, functionInputTypeClass);
        Object evaluate = reportFunction.evaluate(transactionReportInstruction);
        return OBJECT_MAPPER.writeValueAsString(evaluate);
    }
}