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
    private int countData;
    private String text;
    private StringBuilder bigData;
    int remaining;
    int count;
    int packageSize;
    int wholeSize;
    int rounds;

    @Override
    public void start(){
        initialize(); // Initialisieren von Variablen
        //calculate how many packages have to be sent to reach the whole data volume
        calculateRounds();
        bus.registerHandler("start.reading.data", new Handler<Message>() {
            @Override
            public void handle(Message event) {
                count ++;
                if (count > rounds){
                    bus.send("splitData.finish", "finish"); // Sendet finish an Adresse splitData.finish sobald der Text 120 mal eingelesen wurde
                    container.logger().trace("end:finishing reading " + (wholeSize/packageSize)*packageSize);
                    System.exit(0);
                }
                text = "";
                try {
                    //unique identifier
                    final String uuid = UUID.randomUUID().toString();
                    container.logger().trace("startReading:" + uuid);
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
                    //8377236 = 16MB
                    //hänge text als ganzes so lange an, bis die größe fast erreicht ist
                    for (int i=0; i<(packageSize-remaining)/text.length(); i++){
                        //pdf file should be read every time from new so that the program behaves like in the reality
                        readText();
                        bigData.append(text);
                    }
                    //errechne durch modulo wieviel text genau zu z.B. 8mb fehlen
                    readText();
                    bigData.append(text.substring(0, (packageSize-remaining)%text.length()));
                    remaining = text.length() - (packageSize-remaining)%text.length();
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

    //calculates how many packages has to be sent (abgerundet)
    private void calculateRounds(){
        rounds = wholeSize / packageSize;
    }

    private void initialize(){
        bus = vertx.eventBus();
        log = container.logger();
        bigData = new StringBuilder();
        count = 0;
        wholeSize = container.config().getInteger("wholeSize");
        packageSize = container.config().getInteger("packageSize");
        countData = 0; // Anzahl an Characters/words die eingelesen werden -> Davon hängt die Datengröße dann ab
        rounds = 0;
        remaining = 0;
    }
}