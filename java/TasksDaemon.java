import software.amazon.awssdk.services.sqs.model.Message;

import java.io.File;
import java.io.FileWriter;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public  class TasksDaemon implements Runnable {
    static final String outputBucket = "dsp-ass1-na-output";
    AtomicInteger totalMsgCount;
    public TasksDaemon(AtomicInteger totalMsgCount){
        this.totalMsgCount=totalMsgCount;
    }

    @Override
    public void run() {
            try{
            List<String> processedImagesQueues = SQSMethods.listQueuesFilter("processedImagesQueue");
            for (String processedImagesQueue : processedImagesQueues) {
                List<Message> messagesInQueue = SQSMethods.receiveAllMessages(processedImagesQueue);
                int messagesRequired = Integer.parseInt(SQSMethods.getQueueTotalMsgsTag(processedImagesQueue));
                System.out.printf("Total messages required are %d and messages in queue now are %d", messagesRequired, messagesInQueue.size());
                if (messagesInQueue.size() == messagesRequired) {
                    String taskId = processedImagesQueue.split("_____")[1];
                    String clientQueueUrl = SQSMethods.getClientQueueUrlTag(processedImagesQueue);
                    handleResponseToClient(clientQueueUrl, taskId, processedImagesQueue, messagesInQueue);
                }
            }
    }
        catch (Exception e){e.printStackTrace();
            }
    }


    private static void handleResponseToClient(String clientQueueUrl,String taskId,String proccessedImagesQueue,List<Message> proccesesImagesMsgs) {
        try {
            File summaryFile = File.createTempFile("summary", ".tmp");
            FileWriter writer = new FileWriter(summaryFile);
            for (Message msg : proccesesImagesMsgs) {
                writer.write("<p>\n <br>\n <img src="+msg.messageAttributes().get("Extra").stringValue()+">\n <br>\n");
                writer.write(msg.body()+"\n </p>\n");
                SQSMethods.deleteMessageAsync(proccessedImagesQueue, msg);
            }
            writer.close();
            String outputKey = taskId+ ".txt";
            S3Methods.uploadFileAsync(outputBucket, outputKey, summaryFile);
            SQSMethods.sendMessage(clientQueueUrl, outputKey);
            SQSMethods.deleteSQSQueueAsync(proccessedImagesQueue);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}