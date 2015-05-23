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
    int size;

    @Override
    public void start(){
        initialize(); // Initialisieren von Variablen
        bus.registerHandler("start.reading.data", new Handler<Message>() {
            @Override
            public void handle(Message event) {
                text = "";
                try {
                    final String uuid = UUID.randomUUID().toString();
                    container.logger().trace("startReading:" + uuid);
                    text = readText();
                    bus.send(container.config().getString("address"), uuid);
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
                    for (int i=0; i<(size-remaining)/text.length(); i++){
                        bigData.append(text);
                    }
                    bigData.append(text.substring(0, (size-remaining)%text.length()));
                    remaining = text.length() - (size-remaining)%text.length();
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
                    //hänge text als ganzes so lange an, bis die größe fast erreicht ist
                    for (int i=0; i<(size-remaining)/text.length(); i++){
                        //pdf file should be read every time from new so that the program behaves like in the reality
                        readText();
                        bigData.append(text);
                    }
                    //errechne durch modulo wieviel text genau zu z.B. 8mb fehlen
                    readText();
                    bigData.append(text.substring(0, (size-remaining)%text.length()));
                    remaining = text.length() - (size-remaining)%text.length();
                    //prüfe ob zufällig die letzen buchstaben ein wort abschließen
                    if (bigData.toString().charAt(bigData.length()-1) == ' '){
                        bus.send("splitData.address", bigData.toString());
                    } else {
                        --remaining;
                        //hänge solange einen Buchstaben an, bis ein ganzes Wort bigData abschließt
                        while(true){
                            if (text.charAt(text.length()-remaining) == ' '){
                                //concatinate uuid
                                container.logger().trace("endReading:" + message.body());
                                bus.send("splitData.address", bigData.toString() + "#START##ID#" + message.body());
                                log.trace("#ID#" + UUID.randomUUID().toString());
                                log.info("Size of bigData reached.");
                                break;

                            } else {
                                bigData.append(text.charAt(text.length()-remaining));
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

    public String readText() throws Exception{
        PDDocument document = PDDocument.load(container.config().getString("documentPath")); // Hier wird die PDF Datei vom Pfad in die Variable document geladen. Der Pfad wird mittels Konfigurationsdatei definiert

        PDFTextStripper stripper = new PDFTextStripper();  // Hilfsklasse der Klasse PDFTextStripper, welche das Einlesen der PDF durchführt
        String text = stripper.getText(document); // Hier wird die Methode getText der Klasse PDFTextStripper aufgerufen welche den Text der Datei als String zurückgibt
        document.close();
        return text;
    }
    private void initialize(){
        bus = vertx.eventBus();
        log = container.logger();
        bigData = new StringBuilder();
        size = container.config().getInteger("dataSize");
        count = 0; // Anzahl wie oft eingelesen wird
        countData = 0; // Anzahl an Characters/words die eingelesen werden -> Davon hängt die Datengröße dann ab
    }
}