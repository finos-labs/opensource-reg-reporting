package org.finos.orr;

import com.fasterxml.jackson.core.JsonProcessingException;
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

public class SparkValidationRunner {
    public static final ObjectMapper OBJECT_MAPPER = RosettaObjectMapperCreator.forJSON().create();

    public static void main(String[] args) {
        if (args.length != 4) {
            printUsage();
            return;
        }
        String runName = args[0];
        String inputType = args[1];
        String databaseName = args[2];
        String inputTable = args[3];

        SparkSession spark = SparkSession.builder().appName(runName).getOrCreate();

        UDF1<VariantVal, List<Map<String, String>>> runValidations = (jsonInput) -> runValidations(jsonInput.toString(), inputType);
        spark.udf().register(runName, runValidations, DataTypes.createArrayType(DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)));

        Dataset<Row> df = spark.read().table("%s.%s".formatted(databaseName, inputTable))
                .withColumn("validations", callUDF(runName, col("data")))
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

        df.write().mode(SaveMode.Overwrite).option("mergeSchema", "true").saveAsTable("%s.%s".formatted(databaseName, runName + "_validation"));
    }

    private static void printUsage() {
        System.out.println("Usage: SparkReportRunner <run-name> <function-name> <function-input-type> <catalog.schema> <input-table>");
        System.out.println("""
                Example: ["cftc_part_43",
                          "drr.regulation.cftc.rewrite.CFTCPart45TransactionReport",
                          "opensource_reg_reporting.orr",
                          "cftc_part_43_report_json"]
                """);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static List<Map<String, String>> runValidations(String jsonInput, String inputType) throws JsonProcessingException, ClassNotFoundException {
        Injector injector = Guice.createInjector(new DrrRuntimeModule());
        RosettaTypeValidator rosettaTypeValidator = injector.getInstance(RosettaTypeValidator.class);

        RosettaModelObject object = (RosettaModelObject) OBJECT_MAPPER.readValue(jsonInput, Class.forName(inputType));
        ValidationReport validationReport = rosettaTypeValidator.runProcessStep(object.getType(), object);
        List<ValidationResult<?>> validationResults = validationReport.getValidationResults();

        List<Map<String, String>> res = new ArrayList<>();
        for (ValidationResult<?> validationResult : validationResults) {
            HashMap<String, String> validations = new HashMap<>();
            validations.put("validation_name", validationResult.getName().replace(object.getType().getSimpleName(), ""));
            validations.put("definition", validationResult.getDefinition());
            validations.put("path", validationResult.getPath().toString());
            validations.put("status", validationResult.isSuccess() ? "PASS" : "FAIL");
            validations.put("failure", validationResult.getFailureReason().orElse("NONE"));
            validations.put("validation_type", validationResult.getValidationType().toString());
            res.add(validations);
        }

        return res;
    }
}
