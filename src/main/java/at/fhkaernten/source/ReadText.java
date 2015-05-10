package at.fhkaernten.source;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Verticle;

/**
 * Created by Christian on 03.03.2015.
 * Hier wird die PDF Datei zur weiteren Verarbeitung eingelesen
 */
public class ReadText extends Verticle {
    private Logger log;
    private EventBus bus;
    private int count;
    private int countData;
    private String text;
    private StringBuilder bigData;
    int remaining = 0;
    @Override
    public void start(){
        bus = vertx.eventBus();
        log = container.logger();
        bigData = new StringBuilder();
        count = 0; // Anzahl wie oft pdf eingelesen wird
        countData = 0; // Anzahl an Characters/words die eingelesen werden -> Davon hängt die Datengröße dann ab
        bus.registerHandler("start.reading.data", new Handler<Message>() {
            @Override
            public void handle(Message event) {
                bus.send("initial.address", container.config().getString("address"));
                text = "";
                try {
                    PDDocument document = PDDocument.load(container.config().getString("documentPath")); // Hier wird die PDF Datei vom Pfad in die Variable document geladen. Der Pfad wird mittels Konfigurationsdatei definiert

                    PDFTextStripper stripper = new PDFTextStripper();  // Hilfsklasse der Klasse PDFTextStripper, welche das Einlesen der PDF durchführt
                    text = stripper.getText(document); // Hier wird die Methode getText der Klasse PDFTextStripper aufgerufen welche den Text der Datei als String zurückgibt
                    document.close();
                    bus.send(container.config().getString("address"), "start");
                    bus.send("splitData.finish", "finish"); // Sendet finish an Adresse splitData.finish sobald der Text 120 mal eingelesen wurde
                    container.logger().info("Data has been processed");
                } catch (Exception e) {
                    container.logger().error("Reading the PDF File failed " + container.config().getString("documentPath") );
                }
            }
        });

        bus.registerHandler("keep.reading.char", new Handler<Message<String>>() {
            @Override
            public void handle(Message<String> message) {
                bigData.setLength(0);
                if (remaining != 0){
                    bigData.append(text.substring(text.length()-remaining, text.length()-1));
                }
                try {
                    //8377236 = 16MB
                    for (int i=0; i<(8377236-remaining)/text.length(); i++){
                        bigData.append(text);
                    }
                    bigData.append(text.substring(0, (8377236-remaining)%33243));
                    remaining = 33243 - (8377236-remaining)%33243;
                    bus.send("splitData.address", bigData.toString()); // Holt sich einen Teil vom gesamten String
                } catch (Exception e){
                    int remainingChar = countData=-15;
                    StringBuilder concatText = new StringBuilder();
                    concatText.append(bigData.toString().substring(remainingChar, text.length() - 1));
                    //concatText.append())
                    String temp = bigData.toString().substring(--countData * 15, text.length()-1);
                    bus.send("splitData.address", temp);
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
                    //8377236 = 16MB
                    for (int i=0; i<(8377236-remaining)/text.length(); i++){
                        bigData.append(text);
                    }
                    bigData.append(text.substring(0, (8377236-remaining)%33243));
                    remaining = 33243 - (8377236-remaining)%33243;

                    if (bigData.toString().charAt(bigData.length()-1) == ' '){
                        bus.send("splitData.address", bigData.toString());
                    } else {
                        --remaining;
                        while(true){
                            if (text.charAt(33243-remaining) == ' '){
                                bus.send("splitData.address", bigData.toString());
                                log.info("dhksa");
                                break;

                            } else {
                                bigData.append(text.charAt(33243-remaining));
                                remaining--;
                            }
                        }
                    }
                } catch (Exception e){
                    log.error(e);
                }
            }
        });
    }
}