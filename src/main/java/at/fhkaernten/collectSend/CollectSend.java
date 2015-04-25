package at.fhkaernten.collectSend;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.net.NetClient;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.platform.Verticle;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class CollectSend extends Verticle {
    private EventBus bus;
    private Logger log;
    private NetClient client;
    private StringBuilder batchString;
    private JsonArray arrayOfPorts;
    private ConcurrentMap<String, String> sharedMap;
    private Map<String, String> deploymentMap;
    @Override
    public void start(){

        bus = vertx.eventBus();
        log = container.logger();
        batchString = new StringBuilder();
        arrayOfPorts = container.config().getArray("Port_Of_Hosts");
        sharedMap = vertx.sharedData().getMap("pingMap");
        client = vertx.createNetClient();
        deploymentMap = new HashMap<>();

        //warte alle reply ping ab (1 Sekunde)
        vertx.setTimer(1, new Handler<Long>() {
            @Override
            public void handle(Long event) {
                bus.send("start.reading.data", "start");
            }
        });

        bus.registerHandler("splitData.address", new Handler<Message<String>>() {
            @Override
            public void handle(Message<String> message) {
                // Hängt gesendete Character von ReadText an batchString an
                //batchString.append(message.body()); // Body = <Character> in Zeile 64
                //if (batchString.length() >= 160 / 2) {
                    final String charBuffer = message.body();
                    batchString.setLength(0);
                    //Start Timer
                    //vertx.setPeriodic(1000 * 2, new Handler<Long>() {
                    //@Override public void handle(Long event) {
                    getSharedMap();
                    boolean check = true;
                    for (Object obj : arrayOfPorts) {
                        if (((JsonObject)obj).getBoolean("reachable")) {
                            check = false;
                            ((JsonObject)obj).putBoolean("reachable", false);
                            setSharedMap();
                            final String remoteAddress = ((JsonObject)obj).getString("remoteAddress");
                            container.deployVerticle("at.fhkaernten.collectSend.ReceiveData", new JsonObject("{\"port\":" + ((JsonObject)obj).getInteger("port") + "," +
                                            "\"remoteAddress\":\"" + remoteAddress + "\"}"),
                                    new AsyncResultHandler<String>() { // Sobald ein Event passiert ist (Verticle deployed), springt das Programm hier hin
                                        @Override
                                        public void handle(AsyncResult<String> asyncResult) {
                                            if(asyncResult.succeeded()){
                                                deploymentMap.put(remoteAddress, asyncResult.result());
                                                bus.send(remoteAddress, charBuffer);
                                                bus.send("keep.reading", "continue reading");
                                            }

                                        } // handle
                                    });


                            //writeToSocket(((JsonObject)obj).getInteger("port"), charBuffer);
                            break;
                        }
                    } //for
                    if (check){
                        bus.send("splitData.address", charBuffer);
                    }
                    //}}); //timer

                //}//if
            }//handle
        });

        bus.registerHandler("finish", new Handler<Message<String>>() {
            @Override
            public void handle(Message<String> message) {
                container.undeployVerticle(deploymentMap.get(message.body()));
                deploymentMap.remove(message.body());
            }
        });
        bus.registerHandler("splitData.finish", new Handler<Message<String>>() {
            @Override
            //
            public void handle(Message<String> message) {
                System.out.println("Finish");
            }
        });
    }

    private void writeToSocket(int portNumber, final String charBuffer){
        client.connect(++portNumber, "localhost", new Handler<AsyncResult<NetSocket>>() {
            @Override
            public void handle(AsyncResult<NetSocket> event) {
                event.result().exceptionHandler(new Handler<Throwable>() {
                    @Override
                    public void handle(Throwable throwable) {
                        log.error("Connection could not be established to " + container.config().getString("name") + " with  " + container.config().getInteger("port"));
                    }
                });
                if (event.succeeded()) {
                    log.info("Connected to host " + container.config().getString("name") + " with " + container.config().getInteger("port") + " and ready to send data.");
                    event.result().write(charBuffer);
                    //event.result().close();
                }
            }
        });
    }

    private void setSharedMap(){
        for (Object obj : arrayOfPorts){
            String hostName = ((JsonObject) obj).getString("name");
            sharedMap.put(hostName, ((JsonObject) obj).encode());
        }
    }

    private void getSharedMap(){
        for (Map.Entry<String, String> entry : sharedMap.entrySet()) {
            JsonObject newHostConfig = new JsonObject(entry.getValue());
            for (Object obj : arrayOfPorts){
                JsonObject oldHostConfig = (JsonObject) obj;
                if (newHostConfig.getString("name").equals(oldHostConfig.getString("name"))){
                    oldHostConfig.putBoolean("reachable", newHostConfig.getBoolean("reachable"));
                    break;
                }
            }
        }
    }
    @Override
    public void stop(){
        client.close();
    }
}

