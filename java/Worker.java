import net.sourceforge.lept4j.util.LoadLibs;
import net.sourceforge.tess4j.Tesseract;
import net.sourceforge.tess4j.TesseractException;
import software.amazon.awssdk.services.sqs.model.Message;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public final class Worker  {
    static final String imagesToProcessQueue = "https://sqs.us-east-1.amazonaws.com/579568785877/ImagesToProcessQueue";
    static final String failedMessagesQueue ="https://sqs.us-east-1.amazonaws.com/579568785877/FailedMessagesQueue";


    public static void  main(String[] args)  {
        //Tesseract tesseract = new Tesseract();
        boolean terminate = false;
            System.out.println("Worker Starting");
            //tesseract.setDatapath("./Tess4J/tessdata");
        while (true) {
                List<Message> messages = SQSMethods.receiveMessages(imagesToProcessQueue,"Extra");
                for (Message msg : messages) {
                    String processedImagesQueue = msg.messageAttributes().get("Extra").stringValue();
                    try {
                        URL url = new URL(msg.body());
                        BufferedImage image = ImageIO.read(url);
                        CompletableFuture.delayedExecutor(5, TimeUnit.SECONDS).execute(() -> {
                        //String text = tesseract.doOCR(image);
                        SQSMethods.sendMessage(processedImagesQueue, "Danke brudaaa", msg.body());
                        SQSMethods.deleteMessageAsync(imagesToProcessQueue, msg);
                        });
                    } catch (IOException  e) {
                        String exceptionDetails = e.getMessage() + '\n' + e.getCause().getMessage();
                        SQSMethods.sendMessage(processedImagesQueue, exceptionDetails, msg.body());
                        SQSMethods.deleteMessageAsync(imagesToProcessQueue, msg);
                        e.printStackTrace();
                    }
                }
        }
    }
}
