package at.fhkaernten;
/*
 *
 */
import org.apache.commons.io.IOUtils;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;
import java.io.IOException;
import java.io.InputStream;

/*
Aufruf von Main wird über die Datei mod.json deklariert -> Ist der Startpunkt des Programmes.
In der Main Klasse werden die Verticles mit ihren JSON Konfigurationsdateien deployed.
Die JSON Konfigurationsonsdateien finden sich im Resources Ordner mit equivalentem Dateinamen = Klassennamen
 */
public class Main extends Verticle {
    @Override
  public void start() {
        deployVerticle("at.fhkaernten.source.ReadText"); // Hier wird die Methode deployVerticle aufgerufen
        deployVerticle("at.fhkaernten.collectSend.PingVerticle");
        deployWorkerVerticle("at.fhkaernten.collectSend.CollectSend");
    }

    private void deployVerticle(final String classname) {
        try {
            container.deployVerticle( //Container beinhaltet alle Threads in VertX
                    classname, // = "at.fhkaernten.source.ReadText" bzw. "at.fhkaernten.collectSend.CollectSend"
                    getConfigs(classname), // Hier wird die Methode getConfigs aufgerufen und die JSON Konfigurationsdatei zurückgegeben
                    1, // Anzahl an gleichzeitigen Verticles
                    new AsyncResultHandler<String>() { // Sobald ein Event passiert ist (Verticle deployed), springt das Programm hier hin
                        @Override
                        public void handle(AsyncResult<String> asyncResult) {
                            container.logger().info(String.format("Verticle %s has been deployed.", classname));
                        } // handle
                    } // handler
            );
        } catch (Exception e) {
            container.logger().error("Failed to deploy "+classname, e);
        }
    } // deployVerticle

    private void deployWorkerVerticle(final String classname) {
        try {
            container.deployWorkerVerticle( //Container beinhaltet alle Threads in VertX
                    classname, // = "at.fhkaernten.source.ReadText" bzw. "at.fhkaernten.collectSend.CollectSend"
                    getConfigs(classname), // Hier wird die Methode getConfigs aufgerufen und die JSON Konfigurationsdatei zurückgegeben
                    1, // Anzahl an gleichzeitigen Verticles
                    false,
                    new AsyncResultHandler<String>() {
                        @Override
                        public void handle(AsyncResult<String> event) {
                            container.logger().info(String.format("Verticle %s has been deployed.", classname));
                        }
                    }
            );
        } catch (Exception e) {
            container.logger().error("Failed to deploy "+classname, e);
        }
    } // deployVerticle

    /**
     * Die Methode JsonObject gibt das Objekt "Config" aus der entsprechenden JSON Konfigurationsdabei zurück.
     * @param classname
     * @return
     * @throws IOException
     */
    private static JsonObject getConfigs(String classname) throws IOException {
        // Hier werden die Punkte aus at.fhkaernten. durch / ersetzt um einen korrekten Pfad zur Konfigurationsdatei zu erhalten
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(classname.replaceAll("\\.", "/")+".json");

        // Hier wird eine neue Variable config angelegt (des Typs JsonObject), in welcher der Inhalt des InputStreams is gespeichert wird
        JsonObject config = new JsonObject(IOUtils.toString(is, "UTF-8"));
        // Hier wird das Objekt innerhalb der Json Klasse ausgelesen und mittels return an die Methode zurückgegeben (alles was rechts vom Doppelpunkt steht; Links = Keyword, Rechts: Inhalt)
        JsonObject c = config.getObject("config");

        return c;
    } // getConfigs
}
