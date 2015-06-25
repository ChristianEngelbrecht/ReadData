package at.fhkaernten;

import org.apache.commons.io.IOUtils;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;
import java.io.IOException;
import java.io.InputStream;

/**
Call of class Main is done through a declaration in file resources/mod.json -> This is the entry point of the program.
The main class is used to deploy verticles (with included JSON configuration file -> resources/<NameOfVerticle>.json
 **/
public class Main extends Verticle {
    @Override
  public void start() {
        deployWorkerVerticle("at.fhkaernten.source.ReadText", 1);
        deployVerticle("at.fhkaernten.collectSend.PingVerticle", 1);
        deployWorkerVerticle("at.fhkaernten.collectSend.CollectSend", 1);
    }

    private void deployVerticle(final String classname, int numberOfThreads) {
        try {
            container.deployVerticle(
                    classname, // = e.g.: "at.fhkaernten.source.ReadText"
                    getConfigs(classname),
                    numberOfThreads, // Number of verticle instances
                    new AsyncResultHandler<String>() {
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

    private void deployWorkerVerticle(final String classname, int numberOfThreads) {
        try {
            JsonObject config = getConfigs(classname);
            // Override default configuration -> External configuration will be called via command line
            config.putNumber("packageSize", container.config().getNumber("packageSize"));
            config.putNumber("wholeSize", container.config().getNumber("wholeSize"));
            container.deployWorkerVerticle(
                    classname,
                    config,
                    numberOfThreads,
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
     * The method getConfigs returns configurations as a JsonObject
     */
    private static JsonObject getConfigs(String classname) throws IOException {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(classname.replaceAll("\\.", "/")+".json");
        JsonObject config = new JsonObject(IOUtils.toString(is, "UTF-8"));
        JsonObject c = config.getObject("config");
        return c;
    } // getConfigs
}
