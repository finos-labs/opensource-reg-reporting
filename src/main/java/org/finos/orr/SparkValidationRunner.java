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
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createMapType;

public class SparkValidationRunner {
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

        UDF1<VariantVal, List<Map<String, String>>> runValidations = (jsonInput) -> runValidations(jsonInput.toString(), functionName);
        spark.udf().register(outputTable, runValidations, DataTypes.createArrayType(createMapType(StringType, StringType)));

        Dataset<Row> df = spark.read().table("%s.%s".formatted(databaseName, inputTable)).select(
                        col("identifier"),  col("name"), col("data"))
                .withColumn("validations", callUDF(outputTable, col("data")))
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

        df.write().mode(SaveMode.Overwrite).option("mergeSchema", "true").saveAsTable("%s.%s".formatted(databaseName, outputTable));
    }

    private static void printUsage() {
        System.out.println("Usage: <output-table> <input-table> <function-name> <catalog.schema>");
        System.out.println("""
                Example: ["cftc_part_43_validation",
                          "cftc_part_43_report",
                          "drr.regulation.cftc.rewrite.reports.CFTCPart45ReportFunction",
                          "opensource_reg_reporting.orr"]
                """);
    }

    public static List<Map<String, String>> runValidations(String jsonInput, String functionName) throws JsonProcessingException, ClassNotFoundException {
        Injector injector = Guice.createInjector(new DrrRuntimeModule());
        RosettaTypeValidator rosettaTypeValidator = injector.getInstance(RosettaTypeValidator.class);

        Class<RosettaModelObject> functionReturnType = OrrUtils.getFunctionReturnType(OrrUtils.getFunctionClass(functionName));

        RosettaModelObject object = OBJECT_MAPPER.readValue(jsonInput, functionReturnType);
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
