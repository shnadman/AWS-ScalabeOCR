
import com.google.gson.Gson;
import org.apache.commons.io.FileUtils;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;


import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;


public final class AsyncTest {

    static final String s3InputFilePathQueue = "https://sqs.us-east-1.amazonaws.com/579568785877/NewClientQueue";
    static final String proccessedImagesQueue = "https://sqs.us-east-1.amazonaws.com/579568785877/ProccessedImagesQueue";

    public static void main(String[] args) throws IOException {
        String inputBucket = "dsp-ass1-na-input";
        String key ="images.txt";
        while (true) {
            CompletableFuture<ResponseBytes<GetObjectResponse>> future = S3Methods.getObjectAsync(inputBucket, key);
            future.thenApplyAsync((resp) -> extractUrlsFromInputStream(resp.asInputStream())).thenAccept((urls) -> System.out.println("d"));
        }


    }

    private static List<String> extractUrlsFromInputStream(InputStream input) {
        List<String> urls = new ArrayList<>();

        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(input));
            String line = null;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                urls.add(line);
            }

            return urls;
        }
        catch (Exception e) {
            return urls;
        }
    }


}

