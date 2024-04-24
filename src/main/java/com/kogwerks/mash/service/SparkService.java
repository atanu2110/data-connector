package com.kogwerks.mash.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kogwerks.mash.dto.ColumnStatisticDto;
import com.kogwerks.mash.dto.DistributionDto;
import com.kogwerks.mash.dto.TableProfileDto;
import com.kogwerks.mash.dto.TableSchemaDto;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@AllArgsConstructor
public class SparkService {

    ObjectMapper mapper = new ObjectMapper();

   /* public TableProfileDto runJob() {
        // Initialize Spark configuration
        //SparkConf conf = new SparkConf().setAppName("SparkPostgresExample").setMaster("local[*]");

      *//*System.setProperty("hadoop.home.dir", "C:/Users/atanu/Desktop/Atanu/softwares/Hadoop");
        System.load("C:/Users/atanu/Desktop/Atanu/softwares/Hadoop/bin/hadoop.dll");*//*

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
        log.info("Schemaaaaa");
         data.printSchema();
        // Convert schema to JSON
        String schemaJson = data.schema().json();

        // Convert data to JSON
       // String dataJson = data.toJSON().collectAsList().toString();

        // Construct JSON response
        String schemaResponse = String.format("{\"schema\": %s}", schemaJson);

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
*//*
        long missingValuesCount = data.filter(row -> row.anyNull()).count();
        System.out.println("Number of missing values: " + missingValuesCount);*//*

        double[] percentiles = {0.01, 0.99};
        var quantiles = data.stat().approxQuantile("unit_price", percentiles, 0.0);
        var lower_bound = quantiles[0];
        var upper_bound = quantiles[1];

        log.info("Lower Bound : {}", lower_bound);
        log.info("Upper Bound : {}", upper_bound);
        //var outliers = data.filter((col("unit_price") == lower_bound) || (data["unit_price"] > upper_bound));
        var outliers = data.filter(data.col("unit_price").$less$eq(lower_bound)
            .or(data.col("unit_price").$greater$eq(upper_bound)));

        log.info("Outlierssss");
        outliers.show();

        // Convert data to JSON
        String outliersJson = outliers.toJSON().collectAsList().toString();

        // Construct JSON response
        String outliersResponse = String.format("{\"data\": %s}", outliersJson);

        // Check data distribution for numerical columns
        // Profile the data to get summary statistics for each column,
        // such as mean, median, standard deviation, minimum, and maximum values.
        log.info("Describeeeeee");
          data.describe().show();
        // Convert data to JSON
        String statsJson = data.describe().toJSON().collectAsList().toString();

        // Construct JSON response
        String statsResponse = String.format("{\"data\": %s}", statsJson);

        //Categorical value distributions
        data.groupBy("type").count().show();


        // Calculate total number of rows
        long totalRows = data.count();

        // Iterate over DataFrame schema to calculate non-null count for each column
        log.info("Data Quality Percentage:");
        for (String columnName : data.columns()) {
            // Calculate non-null count for current column
            Column nonNullCount = functions.sum(functions.when(data.col(columnName).isNotNull(), 1).otherwise(0));
            long nonNullCountValue = data.select(nonNullCount).first().getLong(0);

            // Calculate data quality percentage for current column
            double dataQualityPercentage = (nonNullCountValue / (double) totalRows) * 100;

            // Print data quality percentage for current column
            log.info("Data Quality Percentage for " + columnName + ": " + dataQualityPercentage + "%");
        }


        // Iterate over DataFrame columns to detect anomalies
       *//* System.out.println("Anomalies:");
        for (String columnName : data.columns()) {
            // Calculate mean and standard deviation for current column
            double mean = data.agg(functions.mean(columnName)).head().getDouble(0);
            double stddev = data.agg(functions.stddev(columnName)).head().getDouble(0);

            // Calculate Z-score for each value in current column
            Dataset<Row> zScores = data.withColumn("zScore", functions.abs(data.col(columnName).minus(mean).divide(stddev)));

            // Filter rows with zScore > threshold (e.g., 3 for 3 standard deviations)
            double threshold = 3; // Adjust threshold as needed
            Dataset<Row> anomalies = zScores.filter(zScores.col("zScore").gt(threshold));

            // Print anomalies for current column
            anomalies.select(columnName).show(false);
        }*//*


     *//* // Impute missing values with mean, median, or mode
        Dataset<Row> dfImputed = df.na().fill(0); // Example: Fill missing values with 0

        // Remove rows with missing values
        Dataset<Row> dfCleaned = df.na().drop();*//*
        TableProfileDto tableProfileDto = TableProfileDto.builder().schema(schemaResponse).missingValue(missingValue)
            .uniqueValue(uniqueValue).stats(statsResponse).outliers(outliersResponse).build();
        // Close Spark session
        spark.stop();

        return tableProfileDto;
    }*/


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
        String tableName = "mytable";

        // Read data from PostgreSQL table into Spark DataFrame
        Dataset<Row> data = spark.read()
            .jdbc(jdbcUrl, tableName, connectionProperties);

        // Show the data
        //data.show();

        // show schema
        log.info("Schemaaaaa");
        data.printSchema();
        // Convert schema to JSON
        String schemaJson = data.schema().json();

        // Convert data to JSON
        // String dataJson = data.toJSON().collectAsList().toString();

        // Construct JSON response
        String schemaResponse = String.format("{\"schema\": %s}", schemaJson);

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
        var quantiles = data.stat().approxQuantile("charges", percentiles, 0.0);
        var lower_bound = quantiles[0];
        var upper_bound = quantiles[1];

        log.info("Lower Bound : {}", lower_bound);
        log.info("Upper Bound : {}", upper_bound);
        //var outliers = data.filter((col("unit_price") == lower_bound) || (data["unit_price"] > upper_bound));
        var outliers = data.filter(data.col("charges").$less$eq(lower_bound)
            .or(data.col("charges").$greater$eq(upper_bound)));

        log.info("Outlierssss");
        outliers.show();

        // Convert data to JSON
        String outliersJson = outliers.toJSON().collectAsList().toString();

        // Construct JSON response
        String outliersResponse = String.format("{\"data\": %s}", outliersJson);

        // Check data distribution for numerical columns
        // Profile the data to get summary statistics for each column,
        // such as mean, median, standard deviation, minimum, and maximum values.
        log.info("Describeeeeee");
        data.describe().show();
        // Convert data to JSON
        String statsJson = data.describe().toJSON().collectAsList().toString();

        // Construct JSON response
        String statsResponse = String.format("{\"data\": %s}", statsJson);

        //Categorical value distributions
        data.groupBy("sex").count().show();

        // Calculate total number of rows
        long totalRows = data.count();

        // Iterate over DataFrame schema to calculate non-null count for each column
        log.info("Data Quality Percentage:");
        for (String columnName : data.columns()) {
            // Calculate non-null count for current column
            Column nonNullCount = functions.sum(functions.when(data.col(columnName).isNotNull(), 1).otherwise(0));
            long nonNullCountValue = data.select(nonNullCount).first().getLong(0);

            // Calculate data quality percentage for current column
            double dataQualityPercentage = (nonNullCountValue / (double) totalRows) * 100;

            // Print data quality percentage for current column
            log.info("Data Quality Percentage for " + columnName + ": " + dataQualityPercentage + "%");
        }

        // Iterate over DataFrame columns to detect anomalies
       /* System.out.println("Anomalies:");
        for (String columnName : data.columns()) {
            // Calculate mean and standard deviation for current column
            double mean = data.agg(functions.mean(columnName)).head().getDouble(0);
            double stddev = data.agg(functions.stddev(columnName)).head().getDouble(0);

            // Calculate Z-score for each value in current column
            Dataset<Row> zScores = data.withColumn("zScore", functions.abs(data.col(columnName).minus(mean).divide(stddev)));

            // Filter rows with zScore > threshold (e.g., 3 for 3 standard deviations)
            double threshold = 3; // Adjust threshold as needed
            Dataset<Row> anomalies = zScores.filter(zScores.col("zScore").gt(threshold));

            // Print anomalies for current column
            anomalies.select(columnName).show(false);
        }*/


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

    public TableSchemaDto getSchema(String tableName) {
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
        tableName = "mytable";

        // Read data from PostgreSQL table into Spark DataFrame
        Dataset<Row> data = spark.read()
            .jdbc(jdbcUrl, tableName, connectionProperties);
        //data.printSchema();
        TableSchemaDto tableSchemaDto = convertSchema(data.schema().json());
        tableSchemaDto.setRows(data.count());
        tableSchemaDto.setSize("184 MB");

        // Check for missing and unique values
        for (String col : data.columns()) {
            // long missingValuesCount = data.filter(data.col(col).isNull()).count();
            long uniqueValuesCount = data.select(col).distinct().count();

            if (uniqueValuesCount != tableSchemaDto.getRows()) {
                tableSchemaDto.getFields().stream().filter(f -> f.getName().equalsIgnoreCase(col)).forEach(fi -> {
                    fi.setUniqueValuesCount(String.valueOf(uniqueValuesCount));
                });
            }
        }

        return tableSchemaDto;
    }


    public Long getDuplicateRows(String tableName) {
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
        tableName = "mytable";

        // Read data from PostgreSQL table into Spark DataFrame
        Dataset<Row> data = spark.read()
            .jdbc(jdbcUrl, tableName, connectionProperties);

        return data.count() - data.dropDuplicates().count();
    }


    public Long getOutliers(String tableName, String columnName) {

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
        tableName = "mytable";

        // Read data from PostgreSQL table into Spark DataFrame
        Dataset<Row> data = spark.read()
            .jdbc(jdbcUrl, tableName, connectionProperties);

        double[] percentiles = {0.01, 0.99};
        var quantiles = data.stat().approxQuantile("charges", percentiles, 0.0);
        var lower_bound = quantiles[0];
        var upper_bound = quantiles[1];

        log.info("Lower Bound : {}", lower_bound);
        log.info("Upper Bound : {}", upper_bound);
        //var outliers = data.filter((col("unit_price") == lower_bound) || (data["unit_price"] > upper_bound));
        var outliers = data.filter(data.col("charges").$less$eq(lower_bound)
            .or(data.col("charges").$greater$eq(upper_bound)));

        return outliers.count();
    }

    public DistributionDto getDistribution(String tableName) {
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
        tableName = "mytable";

        // Read data from PostgreSQL table into Spark DataFrame
        Dataset<Row> data = spark.read()
            .jdbc(jdbcUrl, tableName, connectionProperties);
        //data.printSchema();
        try {
            String[] summaryStrings = data.describe().showString(100, 0, false).split("\n");
            String[] columns = summaryStrings[1].split("\\|");
            List<String> columnsList = Arrays.stream(columns).map(p -> p.trim()).collect(Collectors.toList());

            List<Map<String, String>> rows = new ArrayList<>();

            for (int i = 3; i < summaryStrings.length - 1; i++) {
                String[] parts = summaryStrings[i].split("\\|");

                if (columns.length != parts.length) {
                    log.error("Mismatch in keys and values length at row " + i);
                    continue;
                }
                Map<String, String> rowMap = new LinkedHashMap<>();
                for (int j = 0; j < columns.length; j++) {
                    rowMap.put(columnsList.get(j).trim(), parts[j].trim());
                }

                rows.add(rowMap);
            }


            // Map<String, Map<String, String>> summaryStats = new HashMap<>();
           /* for (int i = 3; i < summaryStrings.length - 1; i++) {
                String[] parts = summaryStrings[i].split("\\|");
                String columnName = parts[1].trim();
                Map<String, String> statsMap = new HashMap<>();
                statsMap.put("mean", parts[2].trim());
                statsMap.put("stddev", parts[3].trim());
                statsMap.put("min", parts[4].trim());
                statsMap.put("max", parts[5].trim());
                statsMap.put("count", parts[6].trim());
                summaryStats.put(columnName, statsMap);
            }*/

            return DistributionDto.builder().columns(columnsList).rows(rows).build();
        } catch (Exception e) {
            return null;
        }
    }

    public ColumnStatisticDto describeColumn(String tableName, String columnName) {

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
        tableName = "mytable";

        // Read data from PostgreSQL table into Spark DataFrame
        Dataset<Row> data = spark.read()
            .jdbc(jdbcUrl, tableName, connectionProperties);

        // Calculate total number of rows
        long totalRows = data.count();

        long missingValuesCount = data.filter(data.col(columnName).isNull()).count();
        long uniqueValuesCount = data.select(columnName).distinct().count();

        ColumnStatisticDto columnStatisticDto = ColumnStatisticDto.builder()
            .missingValuesCount(missingValuesCount != totalRows ? String.valueOf(missingValuesCount) : "")
            .uniqueValuesCount(uniqueValuesCount != totalRows ? String.valueOf(uniqueValuesCount) : "").build();

        //Categorical value distributions
        columnStatisticDto.setCategoricalCount(data.groupBy(columnName).count().showString(100, 0, false));

        log.info("Data Quality Percentage:");
        Column nonNullCount = functions.sum(functions.when(data.col(columnName).isNotNull(), 1).otherwise(0));
        long nonNullCountValue = data.select(nonNullCount).first().getLong(0);

        // Calculate data quality percentage for current column
        double dataQualityPercentage = (nonNullCountValue / (double) totalRows) * 100;

        // Print data quality percentage for current column
        log.info("Data Quality Percentage for " + columnName + ": " + dataQualityPercentage + "%");
        columnStatisticDto.setColumnName(columnName);
        columnStatisticDto.setDataQualityPercentage(dataQualityPercentage);
        return columnStatisticDto;
    }

    // Convert JSON schema string to TableSchema object
    private TableSchemaDto convertSchema(String jsonSchema) {
        try {
            return mapper.readValue(jsonSchema, TableSchemaDto.class);
        } catch (IOException e) {
            log.error("Error in converting table schema : {}", e.getMessage());
            return null;
        }
    }

}
