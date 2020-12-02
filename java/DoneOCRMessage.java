import java.net.URL;

public class DoneOCRMessage {
    private URL url=null;
    private String imageText=null;
    DoneOCRMessage(URL url,String imageText){
    this.imageText=imageText;
    this.url=url;
    }
}
