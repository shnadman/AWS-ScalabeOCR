

import java.io.*;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.io.FileUtils;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.sync.RequestBody;

public final class S3Methods {
    private static  S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
    private static  S3AsyncClient s3Async = S3AsyncClient.builder() .region(Region.US_EAST_1) .build();



    public static void uploadFileAsync(String bucket, String key, File file) {

        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();

        // Put the object into the bucket
        CompletableFuture<PutObjectResponse> future = s3Async.putObject(objectRequest,
                AsyncRequestBody.fromFile(file)
        );
//        future.whenComplete((resp, err) -> {
//            try {
//                if (resp != null) {
//                    System.out.println("Object uploaded. Details: " + resp);
//                } else {
//                    // Handle error
//                    err.printStackTrace();
//                }
//            } finally {
//                // Only close the client when you are completely done with it
//                s3Async.close();
//            }
//        });
//
//        future.join();
    }

    public static CompletableFuture<ResponseBytes<GetObjectResponse>> getObjectAsync(String bucketName, String key){

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        CompletableFuture<ResponseBytes<GetObjectResponse>> future = s3Async.getObject(getObjectRequest,
                AsyncResponseTransformer.toBytes());


        return future;
    }


    public static void uploadFile(String bucket, String key, File file){
        System.out.println("Uploading object...");
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key)
                        .build(),
                RequestBody.fromFile(file));
        System.out.println("Upload complete");
    }

    public static void createBucket(String bucket, Region region) {
        s3.createBucket(CreateBucketRequest
                .builder()
                .bucket(bucket)
                .createBucketConfiguration(
                        CreateBucketConfiguration.builder()
                                .locationConstraint(region.id())
                                .build())
                .build());

        System.out.println(bucket);
    }

    public static void deleteBucket(String bucket) {
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket).build();
        s3.deleteBucket(deleteBucketRequest);
    }

    public static InputStream getObject(String bucketName, String key){
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();


         return s3.getObject(getObjectRequest);

    }

    public static void closeClient(){
        System.out.println("Closing the connection to Amazon S3");
        s3.close();
        System.out.println("Connection closed");
        System.out.println("Exiting...");
    }

}