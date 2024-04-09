package com.kogwerks.mash.service.impl;

import com.kogwerks.mash.service.DataConnector;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@AllArgsConstructor
public class S3Connection implements DataConnector {
    //private S3Client s3Client;
    @Override
    public void connect() {
       /* // Connect to AWS S3
        try (S3Client s3Client = S3Client.builder()
            .region(Region.US_EAST_1)
            .credentialsProvider(DefaultCredentialsProvider.create())
            .build()) {
            System.out.println("Connected to AWS S3 successfully.");

            ListObjectsRequest listObjectsRequest = ListObjectsRequest.builder()
                .bucket("my-bucket")
                .build();

            ListObjectsResponse listObjectsResponse = s3Client.listObjects(listObjectsRequest);
            for (S3Object s3Object : listObjectsResponse.contents()) {
                System.out.println("S3 Object: " + s3Object.key() + " Size: " + s3Object.size());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }*/
    }

    @Override
    public void close() {
        /*if (s3Client != null) {
            s3Client.close();
            System.out.println("AWS S3 connection closed.");
        }*/
    }

}
