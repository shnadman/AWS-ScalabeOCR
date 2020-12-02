
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


public class Manager {
    static final int MAX_T = 10;
    static final String imagesToProcessQueue= "https://sqs.us-east-1.amazonaws.com/579568785877/ImagesToProcessQueue";
    static final String s3FilePathQueue= "https://sqs.us-east-1.amazonaws.com/579568785877/NewClientQueue";
    static final String terminateQueue= "https://sqs.us-east-1.amazonaws.com/579568785877/TerminateQueue";
    static final String inputBucket = "dsp-ass1-na-input";

    public static void main(String[] args) {
        boolean terminate=false;
        AtomicInteger  totalMsgCount = new AtomicInteger(0);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(new TasksDaemon(totalMsgCount), 3, 1, TimeUnit.SECONDS);
//        ExecutorService executor = Executors.newSingleThreadExecutor();
//        executor.execute(new TasksDaemon(totalMsgCount));

        AtomicInteger taskId= new AtomicInteger(0);
        List<String> activeWorkers = new ArrayList<>();
        EC2Methods.launchWorkerInstances(2,activeWorkers);

        while (!terminate) {
            List<Message> terminateMessages = SQSMethods.receiveMessages(terminateQueue,"Extra");
            if (terminateMessages.size() > 0) terminate = true;
            SQSMethods.receiveMessagesAsync(s3FilePathQueue,"Extra").thenAccept((s3filePaths) -> {
                for (Message s3filePath : s3filePaths.messages()) {
                    taskId.getAndIncrement();
                    String key = s3filePath.body().split("/")[1];
                    String clientQueueUrl = s3filePath.messageAttributes().get("Extra").stringValue();
                    handleClientRequestAsync(inputBucket, key, clientQueueUrl, taskId,activeWorkers,totalMsgCount);
                    SQSMethods.deleteMessageAsync(s3FilePathQueue, s3filePath);
                }
            });
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e){

            }

       }

    }

    private static List<String> extractUrlsFromInputStream(InputStream input)  {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        List<String> urls =new ArrayList<>();
        String line = null;
        while (true) {
            try {
                if ((line = reader.readLine()) == null) break;
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println(line);
            urls.add(line);
        }

        return urls;
    }
    private static void handleClientRequestAsync(String bucket, String key,String clientUrl, AtomicInteger taskId, List<String> activeWorkers, AtomicInteger totalMsgCount){
        int n=3;
        S3Methods.getObjectAsync(bucket,key).thenApply((resp)->extractUrlsFromInputStream(resp.asInputStream())).thenAccept((urls)->{
            Integer messageCount = urls.size()-1;
            int workersNeeded = totalMsgCount.addAndGet(messageCount)/n;
            int workersToCreate = workersNeeded-activeWorkers.size();
//            if(workersToCreate>0){
//            EC2Methods.createWorkerInstances(2,activeWorkers);
//            };
            String processedImagesQueue = SQSMethods.createQueue("processedImagesQueue_____"+taskId,messageCount.toString(),clientUrl);
            SQSMethods.sendBatchURLMessagesWithAttributes(imagesToProcessQueue,urls,processedImagesQueue);
        });
    }
}
