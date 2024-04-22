package com.kogwerks.mash.service.impl;

import com.kogwerks.mash.service.DataConnector;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@AllArgsConstructor
public class MongoDbConnection implements DataConnector {
    //private MongoClient mongoClient;
    @Override
    public List<String> connect() {
        // Connect to MongoDB
//        try (MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017")) {
//            System.out.println("Connected to MongoDB successfully.");
//
//            MongoDatabase database = mongoClient.getDatabase("mydatabase");
//            MongoCollection<Document> collection = database.getCollection("mycollection");
//
//            // Example MongoDB query
//            Document document = collection.find().first();
//            if (document != null) {
//                System.out.println("MongoDB Result: " + document.toJson());
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        return List.of();
    }

    @Override
    public void close() {
       /* if (mongoClient != null) {
            mongoClient.close();
            System.out.println("MongoDB connection closed.");
        }*/
    }
}
