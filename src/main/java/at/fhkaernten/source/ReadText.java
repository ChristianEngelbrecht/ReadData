package at.fhkaernten.source;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Verticle;

import java.util.UUID;

/**
 * Class ReadText is responsible to read PDF file and generate data packets
 */
public class ReadText extends Verticle {
    private Logger log;
    private EventBus bus;
    private int countData;
    private String text;
    private StringBuilder bigData;
    int remaining;
    int count;
    int packageSize;
    long wholeSize;
    long rounds;

    @Override
    public void start(){
        initialize();
        calculateRounds();
        bus.registerHandler("start.reading.data", new Handler<Message>() {

            @Override
            public void handle(Message event) {
                count ++;
                System.out.println(count + " of " + rounds);
                if (count > rounds){
                    bus.send("splitData.finish", "finish");
                    container.logger().info("end:finishing reading " + (wholeSize/packageSize)*packageSize);
                    stop();
                }
                text = "";
                try {
                    // Unique identifier of data package UUID
                    final String uuid = UUID.randomUUID().toString();
                    container.logger().info("startReading:" + uuid);
                    text = readText();
                    bus.send(container.config().getString("address"), uuid);
                    container.logger().info("Data has been processed");
                } catch (Exception e) {
                    container.logger().error("Reading the PDF File failed " + container.config().getString("documentPath") );
                }
            }
        });

        bus.registerHandler("keep.reading.words", new Handler<Message<String>>() {

            @Override
            public void handle(Message<String> message) {
                text = text.replaceAll("[^\\w\\s]", "");
                text = text.replaceAll(System.getProperty("line.separator"), "");
                bigData.setLength(0);
                System.gc();
                if (remaining != 0){
                    try{
                        bigData.append(text.substring(text.length()-remaining, text.length()-1));
                    } catch (Exception e){
                        remaining = 0;
                    }

                }
                try {
                    for (int i=0; i<(packageSize-remaining)/text.length(); i++){
                        // PDF file should be read every time from new so that the program behaves like in the reality - Has not been realized
                        // because reading data took too long
                        // readText();
                        bigData.append(text + " ");
                    }
                    //readText();
                    bigData.append(text.substring(0, (packageSize-remaining)%text.length()));
                    remaining = text.length() - (packageSize-remaining)%text.length();
                    if (bigData.toString().charAt(bigData.length()-1) == ' '){
                        bus.send("splitData.address", bigData.toString() + "#START##ID#" + message.body());
                    } else {
                        --remaining;
                        while(true){
                            if (text.charAt(text.length()-remaining) == ' '){
                                // Concatenate UUID
                                container.logger().info("endReading:" + message.body());
                                bus.send("splitData.address", bigData.toString() + "#START##ID#" + message.body());
                                log.info("#ID#" + UUID.randomUUID().toString());
                                log.info("Size of bigData reached.");
                                break;
                            } else {
                                bigData.append(text.charAt(text.length()-remaining));
                                remaining--;
                            } // if-else
                        } // while
                    }
                } catch (Exception e){
                    log.error(e);
                }
            }
        });
    }

    public String readText() throws Exception{
        PDDocument document = PDDocument.load(container.config().getString("documentPath"));
        PDFTextStripper stripper = new PDFTextStripper();
        String text = stripper.getText(document);
        document.close();
        return text;
    }

    /**
     * Calculates how many packages must be send
     */
    private void calculateRounds(){
        rounds = wholeSize / packageSize;
        log.info("Number of rounds: " + rounds);
    }

    private void initialize(){
        bus = vertx.eventBus();
        log = container.logger();
        bigData = new StringBuilder();
        count = 0;
        wholeSize = container.config().getLong("wholeSize");
        System.out.println("wholeSize " + wholeSize);
        packageSize = container.config().getInteger("packageSize");
        System.out.println("packageSize " + packageSize);
        countData = 0;
        rounds = 0;
        remaining = 0;
    }
}