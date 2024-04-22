package com.kogwerks.mash.service.impl;

import com.kogwerks.mash.service.DataConnector;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
//@AllArgsConstructor
public class SqlConnection implements DataConnector {

    private Connection connection;

    @Override
    public List<String> connect() {
// Connect to SQL database
        // try (Connection sqlConnection = DriverManager.getConnection("jdbc:mysql://localhost:3306/mydatabase", "username", "password")) {

        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/hawks", "admin", "admin");
            log.info("Connected to SQL database successfully.");

            // Example SQL query
            try (Statement statement = connection.createStatement()) {
               // ResultSet resultSet = statement.executeQuery("SELECT CURRENT_DATE");
                ResultSet resultSet = statement.executeQuery("SELECT * FROM pg_catalog.pg_tables order by tablename;");
               List<String> tables = new ArrayList<>();
                while (resultSet.next()) {
                   // log.info("SQL Result: " + resultSet.getString("current_date"));
                    tables.add(resultSet.getString(2));
                }
                resultSet.close();
                return tables;
            } catch (SQLException e) {
                log.error(e.getMessage());
            }
        } catch (SQLException | ClassNotFoundException e) {
            log.error(e.getMessage());
        }
        return List.of();
    }

    @Override
    public void close() {
        if (connection != null) {
            try {
                connection.close();
                log.info("SQL connection closed.");
            } catch (SQLException e) {
                log.error(e.getMessage());
            }
        }
    }

    // table size
    // SELECT pg_size_pretty( pg_total_relation_size('mytable') );
}
