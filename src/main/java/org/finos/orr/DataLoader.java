package org.finos.orr;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class DataLoader {

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.out.println("Usage: DataLoader <run-name> <input-path> <catalog.schema> <output-table>");
            System.out.println("""
                    Example: ["reportable_event",
                              "/Volumes/opensource_reg_reporting/orr/cdm-trades/",
                              "opensource_reg_reporting.orr"]
                    """);
        }
        String outputTable = args[0];
        String inputPath = args[1];
        String databaseName = args[2];

        SparkSession spark = SparkSession.builder().appName(outputTable).getOrCreate();
        List<Row> rows = createRows(inputPath);

        // Define the schema
        StructType schema = new StructType(new StructField[]{
                new StructField("identifier", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("data", DataTypes.StringType, false, Metadata.empty())
        });

        // Create a DataFrame from the list of Rows and the schema
        Dataset<Row> df = spark.createDataFrame(rows, schema);
        df = df.withColumn("data", functions.parse_json(df.col("data")));
        df.write().mode(SaveMode.Overwrite).saveAsTable("%s.%s".formatted(databaseName, outputTable));
    }

    private static List<Row> createRows(String inputPath) throws IOException {
        List<Row> rows = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(Path.of(inputPath))) {
            List<Path> list = walk.filter(Files::isRegularFile).toList();
            for (int i = 0; i < list.size(); i++) {
                Path p = list.get(i);
                rows.add(RowFactory.create(i, p.getFileName().toString(), Files.readString(p)));
            }
        }
        return rows;
    }
}
