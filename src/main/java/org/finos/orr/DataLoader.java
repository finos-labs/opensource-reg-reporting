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

/**
 * Spark application to load data from a directory into a Spark table.
 * The application takes 3 arguments:
 *  - run-name: The name of the run. This will be used as the name of the Spark application.
 *  - input-path: The path to the directory containing the data files.
 *  - catalog.schema: The name of the database/schema to use.
 *
 *  For example:
 *  ["reportable_event",
 *  "/Volumes/opensource_reg_reporting/orr/cdm-trades/",
 *  "opensource_reg_reporting.orr"]
 */
public class DataLoader {

    /**
     * Main method for the Spark application.
     * @param args Command line arguments.
     * @throws IOException If an error occurs while reading the input files.
     */
    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.out.println("Usage: DataLoader <run-name> <input-path> <catalog.schema> <output-table>");
            System.out.println("""
                    Example: ["reportable_event",
                              "/Volumes/opensource_reg_reporting/orr/cdm-trades/",
                              "opensource_reg_reporting.orr"]
                    """);
            return;
        }
        // Read command line arguments
        String outputTable = args[0];
        String inputPath = args[1];
        String databaseName = args[2];

        // Create a Spark session
        SparkSession spark = SparkSession.builder().appName(outputTable).getOrCreate();
        // Create a list of rows from the input files
        List<Row> rows = createRows(inputPath);

        // Define the schema for the table
        StructType schema = new StructType(new StructField[]{
                new StructField("identifier", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("data", DataTypes.StringType, false, Metadata.empty())
        });

        // Create a DataFrame from the list of rows and the schema
        Dataset<Row> df = spark.createDataFrame(rows, schema);
        // Parse the JSON data in the "data" column
        df = df.withColumn("data", functions.parse_json(df.col("data")));
        // Write the DataFrame to the output table
        df.write().mode(SaveMode.Overwrite).saveAsTable("%s.%s".formatted(databaseName, outputTable));
    }

    /**
     * Creates a list of rows from the input files.
     * @param inputPath The path to the directory containing the input files.
     * @return A list of rows.
     * @throws IOException If an error occurs while reading the input files.
     */
    private static List<Row> createRows(String inputPath) throws IOException {
        List<Row> rows = new ArrayList<>();
        // Iterate over the files in the input directory
        try (Stream<Path> walk = Files.walk(Path.of(inputPath))) {
            List<Path> list = walk.filter(Files::isRegularFile).toList();
            for (int i = 0; i < list.size(); i++) {
                Path p = list.get(i);
                // Create a row for each file
                rows.add(RowFactory.create(i, p.getFileName().toString(), Files.readString(p)));
            }
        }
        return rows;
    }
}