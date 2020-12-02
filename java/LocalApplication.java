

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import software.amazon.awssdk.services.sqs.model.Message;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.UUID;



 public final class LocalApplication {

    static final String s3InputFilePathQueue = "https://sqs.us-east-1.amazonaws.com/579568785877/NewClientQueue";
    static final String terminateQueue= "https://sqs.us-east-1.amazonaws.com/579568785877/TerminateQueue";

    public static void main(String[] args) throws IOException {
        String inputFileName = args[0];
        String outputFileName = args[1];
        Integer n = Integer.valueOf(args[2]);
        boolean terminate = false;
        if(args.length>3 && args[3].equals("terminate")){
            terminate=true;
        }
        File file = new File("./"+inputFileName);
        String managerId=null;

        /*
        managerId = EC2Methods.getActiveManagerID();
        if(managerId.equals("None")){
            System.out.println("There wasnt");
            managerId =  EC2Methods.createManagerInstance();
        }
*/

        String inputBucket = "dsp-ass1-na-input";
        String outputBucket = "dsp-ass1-na-output";
        String inputKey = inputFileName;

        S3Methods.uploadFile(inputBucket,inputKey,file);
        String inputFileAddress =inputBucket+"/"+inputKey;
        String queueUrl = SQSMethods.createQueue(UUID.randomUUID().toString());

        SQSMethods.sendMessage(s3InputFilePathQueue,inputFileAddress,queueUrl);
        List<Message> response=null;
        do{
            response =SQSMethods.receiveMessagesLongPoll(queueUrl);
        }
        while(response.size()==0);
        try {
            String outputKey = response.get(0).body();
            InputStream s3FileStream = S3Methods.getObject(outputBucket, outputKey);
            createHtmlFile(s3FileStream, outputFileName);
            SQSMethods.deleteSQSQueue(queueUrl);
        }
        catch (Exception e){
            e.printStackTrace();
        }

        if(terminate){
            SQSMethods.sendMessage(terminateQueue,"Goodbye my lover");
        }

        S3Methods.closeClient();
    }

    private static void createHtmlFile(InputStream s3File, String title){
        try {
            File htmlTemplateFile = new File("./template.html");
            String htmlString = FileUtils.readFileToString(htmlTemplateFile,Charset.defaultCharset());
            String body = IOUtils.toString(s3File, Charset.defaultCharset());
            htmlString = htmlString.replace("$title", title);
            htmlString = htmlString.replace("$body", body);
            File newHtmlFile = new File("./"+title+".html");
            FileUtils.writeStringToFile(newHtmlFile, htmlString,Charset.defaultCharset());
        }
        catch (IOException e){
            e.printStackTrace();
        }

    }
}
