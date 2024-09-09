package org.finos.orr;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.regnosys.rosetta.common.serialisation.RosettaObjectMapper;
import drr.regulation.cftc.rewrite.reports.CFTCPart45ReportFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class SparkRunner {
    
    void run() {

        // Create a SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("JavaSparkExample")
                .getOrCreate();


// Create an Encoder for the Person class
//        Encoder<Person> personEncoder = Encoders.bean(Person.class);

// Create a Dataset from the list of Java objects
//        Dataset<Person> personDataset = spark.createDataset(people, personEncoder);

// Convert the Dataset to a DataFrame
//        Dataset<Row> personDataFrame = personDataset.toDF();

// Show the DataFrame
//        personDataFrame.show();

// Stop the SparkSession
//        spark.stop();

    }
}



