package com.kogwerks.mash.service;

import com.kogwerks.mash.dto.TableProfileDto;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@AllArgsConstructor
public class SparkService {

    public TableProfileDto runJob() {
        // Initialize Spark configuration
        //SparkConf conf = new SparkConf().setAppName("SparkPostgresExample").setMaster("local[*]");

      /*System.setProperty("hadoop.home.dir", "C:/Users/atanu/Desktop/Atanu/softwares/Hadoop");
        System.load("C:/Users/atanu/Desktop/Atanu/softwares/Hadoop/bin/hadoop.dll");*/

        // Create Spark session
        SparkSession spark = SparkSession.builder()
            .appName("Kogwerks-Job").master("local[*]")
            .getOrCreate();

        // JDBC connection properties
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "admin");
        connectionProperties.put("password", "admin");

        // JDBC URL to connect to PostgreSQL
        String jdbcUrl = "jdbc:postgresql://localhost:5432/hawks";

        // Table name in PostgreSQL
        String tableName = "orders";

        // Read data from PostgreSQL table into Spark DataFrame
        Dataset<Row> data = spark.read()
            .jdbc(jdbcUrl, tableName, connectionProperties);

        // Show the data
        //data.show();

        // show schema
        // data.printSchema();
        // Convert schema to JSON
        String schemaJson = data.schema().json();

        // Convert data to JSON
        String dataJson = data.toJSON().collectAsList().toString();

        // Construct JSON response
        String schemaResponse = String.format("{\"schema\": %s, \"data\": %s}", schemaJson, dataJson);

        // Perform operations on the DataFrame if needed
        var duplicateRows = data.count() - data.dropDuplicates().count();
        log.info("NO. of duplicate Rows : {}", duplicateRows);

        // Check for missing values
        Map<String, Long> missingValue = new HashMap<>();
        Map<String, Long> uniqueValue = new HashMap<>();
        for (String col : data.columns()) {
            //  long missingValuesCount = data.filter(data.col(col).isNull().or(data.col(col).equalTo(""))).count();
            long missingValuesCount = data.filter(data.col(col).isNull()).count();
            long uniqueValuesCount = data.select(col).distinct().count();
            log.info("Number of missing values in column " + col + ": " + missingValuesCount);
            missingValue.put(col, missingValuesCount);
            log.info("Number of unique values in column " + col + ": " + uniqueValuesCount);
            uniqueValue.put(col, uniqueValuesCount);
        }
/*
        long missingValuesCount = data.filter(row -> row.anyNull()).count();
        System.out.println("Number of missing values: " + missingValuesCount);*/

        double[] percentiles = {0.01, 0.99};
        var quantiles = data.stat().approxQuantile("unit_price", percentiles, 0.0);
        var lower_bound = quantiles[0];
        var upper_bound = quantiles[1];

        log.info("Lower Bound : {}", lower_bound);
        log.info("Upper Bound : {}", upper_bound);
        //var outliers = data.filter((col("unit_price") == lower_bound) || (data["unit_price"] > upper_bound));
        var outliers = data.filter(data.col("unit_price").$less$eq(lower_bound)
            .or(data.col("unit_price").$greater$eq(upper_bound)));
        //outliers.show();

        // Convert data to JSON
        String outliersJson = outliers.toJSON().collectAsList().toString();

        // Construct JSON response
        String outliersResponse = String.format("{\"data\": %s}", outliersJson);

        // Check data distribution for numerical columns
        // Profile the data to get summary statistics for each column,
        // such as mean, median, standard deviation, minimum, and maximum values.
        //  data.describe().show();
        // Convert data to JSON
        String statsJson = data.describe().toJSON().collectAsList().toString();

        // Construct JSON response
        String statsResponse = String.format("{\"data\": %s}", statsJson);

       /* // Impute missing values with mean, median, or mode
        Dataset<Row> dfImputed = df.na().fill(0); // Example: Fill missing values with 0

        // Remove rows with missing values
        Dataset<Row> dfCleaned = df.na().drop();*/
        TableProfileDto tableProfileDto = TableProfileDto.builder().schema(schemaResponse).missingValue(missingValue)
            .uniqueValue(uniqueValue).stats(statsResponse).outliers(outliersResponse).build();
        // Close Spark session
        spark.stop();

        return tableProfileDto;
    }

}
