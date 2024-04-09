package com.kogwerks.mash.factory;

import com.kogwerks.mash.service.DataConnector;
import com.kogwerks.mash.service.impl.MongoDbConnection;
import com.kogwerks.mash.service.impl.S3Connection;
import com.kogwerks.mash.service.impl.SqlConnection;

public class ConnectorFactory {

    // Factory method to create DataConnector objects
    public static DataConnector createConnector(String type) {
        switch (type.toLowerCase()) {
            case "sql":
                return new SqlConnection();
            case "mongodb":
                return new MongoDbConnection();
            case "s3":
                return new S3Connection();
            default:
                throw new IllegalArgumentException("Invalid connector type: " + type);
        }
    }

}
