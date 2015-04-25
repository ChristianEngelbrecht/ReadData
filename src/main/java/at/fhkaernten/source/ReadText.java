package at.fhkaernten.source;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.net.NetClient;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.platform.Verticle;

/**
 * Created by Christian on 03.03.2015.
 * Hier wird die PDF Datei zur weiteren Verarbeitung eingelesen
 */
public class ReadText extends Verticle {
    private Logger log;
    private EventBus bus;
    private int count;
    private String text;
    @Override
    public void start(){
        bus = vertx.eventBus();
        count = 0;
        bus.registerHandler("start.reading.data", new Handler<Message>() {
            @Override
            public void handle(Message event) {
             text = "";
            try {
                PDDocument document = PDDocument.load(container.config().getString("documentPath")); // Hier wird die PDF Datei vom Pfad in die Variable document geladen. Der Pfad wird mittels Konfigurationsdatei definiert

                PDFTextStripper stripper = new PDFTextStripper();  // Hilfsklasse der Klasse PDFTextStripper, welche das Einlesen der PDF durchführt
                text = stripper.getText(document); // Hier wird die Methode getText der Klasse PDFTextStripper aufgerufen welche den Text der Datei als String zurückgibt
                bus.send("splitData.address", text.substring(count, ++count*1));
                /**for (int i = 0; i < 5; i++) {
                    for (int j = 0; j < text.length(); j++) {
                        if (!text.isEmpty()) {
                            // Sende Character an der Position j an den Event Bus mit der Adresse splitData.address
                            bus.send("splitData.address", text.charAt(j));
                        } else {
                            break;
                        }
                    }
                }**/

                bus.send("splitData.finish", "finish"); // Sendet finish an Adresse splitData.finish sobald der Text 120 mal eingelesen wurde
                container.logger().info("Data has been processed");
            } catch (Exception e) {
                container.logger().error("Reading the PDF File failed " + container.config().getString("documentPath") );
            }
            }
        });

        bus.registerHandler("keep.reading", new Handler<Message<String>>() {
            @Override
            public void handle(Message<String> message) {
                try {
                    bus.send("splitData.address", text.substring(count * 1, ++count * 1));
                } catch (Exception e){
                    bus.send("splitData.address", text.substring(--count * 1, text.length()-1));
                }
            }
        });
    }
}