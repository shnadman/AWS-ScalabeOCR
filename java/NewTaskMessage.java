import java.net.URL;

public class NewTaskMessage {
        private String queueUrl=null;
        private String fileS3Path=null;

    NewTaskMessage(String queueUrl,String fileS3Path){
            this.fileS3Path=fileS3Path;
            this.queueUrl=queueUrl;
        }
}
