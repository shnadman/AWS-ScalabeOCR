
import com.google.gson.Gson;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.CreateTagsRequest;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.net.URL;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;


public final class SQSMethods {
    static final SqsClient sqsClient = SqsClient.builder()
            .region(Region.US_EAST_1)
            .build();

    static final SqsAsyncClient sqsClientAsync = SqsAsyncClient.builder()
            .region(Region.US_EAST_1)
            .build();



    public static String createQueue( String queueName ) {

        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();

        sqsClient.createQueue(createQueueRequest);

        GetQueueUrlResponse getQueueUrlResponse =
                sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
        String queueUrl = getQueueUrlResponse.queueUrl();
        return queueUrl;
    }

    public static void deleteSQSQueue(String queueUrl) {
        try {
            DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                    .queueUrl(queueUrl)
                    .build();
            sqsClient.deleteQueue(deleteQueueRequest);
        } catch (QueueNameExistsException e) {
            e.printStackTrace();
        }
    }

    public static void deleteSQSQueueAsync(String queueUrl) {

            DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                    .queueUrl(queueUrl)
                    .build();
            CompletableFuture<DeleteQueueResponse> future = sqsClientAsync.deleteQueue(deleteQueueRequest);
    }

    public static String createQueue( String queueName,String msgCount,String clientQueueUrl ) {
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();

        sqsClient.createQueue(createQueueRequest);

        GetQueueUrlResponse getQueueUrlResponse =
                sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
        String queueUrl = getQueueUrlResponse.queueUrl();

        final Map<String,String> tags = new HashMap<>();
        tags.put("totalMessages",msgCount);
        tags.put("clietQueueUrl",clientQueueUrl);
        TagQueueRequest tagQueueRequest=  TagQueueRequest.builder().queueUrl(queueUrl).tags(tags).build();
        sqsClient.tagQueue(tagQueueRequest);
        return queueUrl;
    }

    public static String getQueueTotalMsgsTag( String queueUrl) {
        ListQueueTagsRequest listQueueTagsRequest = ListQueueTagsRequest.builder().queueUrl(queueUrl).build();
        ListQueueTagsResponse tagsResponse = sqsClient.listQueueTags(listQueueTagsRequest);
       return  tagsResponse.tags().get("totalMessages");
    }
    public static String getClientQueueUrlTag( String queueUrl) {
        ListQueueTagsRequest listQueueTagsRequest = ListQueueTagsRequest.builder().queueUrl(queueUrl).build();
        ListQueueTagsResponse tagsResponse = sqsClient.listQueueTags(listQueueTagsRequest);
        return  tagsResponse.tags().get("clietQueueUrl");
    }


    public static List<String> listQueuesFilter(String namePrefix ) {
        // List queues with filters
        ListQueuesRequest filterListRequest = ListQueuesRequest.builder()
                .queueNamePrefix(namePrefix).build();

        return sqsClient.listQueues(filterListRequest).queueUrls();

    }

    public static void sendBatchURLMessagesWithAttributes(String queueUrl, List<String> urls, String attribute) {

        final Map<String,MessageAttributeValue> messageAttributes = new HashMap<>();
        MessageAttributeValue attVal = MessageAttributeValue.builder().dataType("String").stringValue(attribute).build();
        messageAttributes.put("Extra",attVal);


        for(String url:urls){
            sqsClient.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageAttributes(messageAttributes)
                    .messageBody(url)
                    .build());
        }
    }

    public static void sendDoneOCRMessage(String queueUrl, URL url, String imageText) {
        DoneOCRMessage doneOCRMessage = new DoneOCRMessage(url,imageText);
        Gson gson = new Gson();
        String json = gson.toJson(doneOCRMessage);

        sqsClient.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(json)
                    .build());
    }

    public static void sendMessage(String queueUrl, String body) {

        sqsClient.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody( body)
                .build());
    }

    public static void sendMessage(String queueUrl, String body, String attribute) {
        final Map<String,MessageAttributeValue> messageAttributes = new HashMap<>();
        MessageAttributeValue attVal = MessageAttributeValue.builder().dataType("String").stringValue(attribute).build();

        messageAttributes.put("Extra",attVal);

        sqsClient.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageAttributes(messageAttributes)
                .messageBody( body)
                .build());
    }



    public static  List<Message> receiveMessages(String queueUrl) {

        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(1)
                .build();
        List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();


        return messages;
    }
    public static  List<Message> receiveMessages(String queueUrl,String attribute) {

        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(1)
                .messageAttributeNames(attribute)
                .waitTimeSeconds(3)
                .build();
        List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();


        return messages;
    }


    public static CompletableFuture<ReceiveMessageResponse> receiveMessagesAsync(String queueUrl,String attribute) {

        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(1)
                .waitTimeSeconds(5)
                .messageAttributeNames(attribute)
                .build();
        CompletableFuture<ReceiveMessageResponse> futureMessages = sqsClientAsync.receiveMessage(receiveMessageRequest);

        return futureMessages;
    }


    public static  List<Message> receiveMessagesLongPoll(String queueUrl) {

        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(10)
                .waitTimeSeconds(20)
                .build();
        List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();

        return messages;
    }

    public static  List<Message> receiveAllMessages(String queueUrl) {

        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(10)
                .messageAttributeNames("Extra")
                .waitTimeSeconds(3)
                .build();
        List<Message> allMsgs = new ArrayList<>();
        int attempts =0;
        try {
            while (attempts <= 5) {
                List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
                if (messages.isEmpty()) {
                    break;
                } else {
                    allMsgs.addAll(messages);
                    attempts++;
                }
            }
        }
        catch (Exception e){e.printStackTrace();}

        return allMsgs;
    }

//    public static  List<Message> receiveAllMessagesAsync(String queueUrl) {
//
//
//
//    }


    public static void deleteMessage( String queueUrl,  Message message) {
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build();
            sqsClient.deleteMessage(deleteMessageRequest);
    }

    public static void deleteMessageAsync( String queueUrl,  Message message) {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();

          sqsClientAsync.deleteMessage(deleteMessageRequest);

    }

    public static void deleteMessages( String queueUrl,  List<Message> messages) {
        for (Message message : messages) {
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build();
            sqsClient.deleteMessage(deleteMessageRequest);
        }
    }


}


