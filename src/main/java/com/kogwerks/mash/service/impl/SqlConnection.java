package com.kogwerks.mash.service.impl;

import com.kogwerks.mash.service.DataConnector;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
//@AllArgsConstructor
public class SqlConnection implements DataConnector {

    private Connection connection;

    @Override
    public void connect() {
// Connect to SQL database
        // try (Connection sqlConnection = DriverManager.getConnection("jdbc:mysql://localhost:3306/mydatabase", "username", "password")) {

        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/hawks", "admin", "admin");
            log.info("Connected to SQL database successfully.");

            // Example SQL query
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery("SELECT CURRENT_DATE");
                while (resultSet.next()) {
                    log.info("SQL Result: " + resultSet.getString("current_date"));
                }
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        if (connection != null) {
            try {
                connection.close();
                log.info("SQL connection closed.");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
