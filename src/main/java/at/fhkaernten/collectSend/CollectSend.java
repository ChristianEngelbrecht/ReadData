package at.fhkaernten.collectSend;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Verticle;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * This class is used to send the produced data from ReadText to the free MapReduce hosts
 */
public class CollectSend extends Verticle {
    private EventBus bus;
    private Logger log;
    private JsonArray arrayOfPorts;
    private ConcurrentMap<String, String> sharedMap;
    private Map<String, String> deploymentMap;

    @Override
    public void start(){
        initialize();
        vertx.setTimer(1000, new Handler<Long>() {

            @Override
            public void handle(Long event) {
                bus.send("start.reading.data", "start");
            }
        });

        bus.registerHandler("splitData.address", new Handler<Message<String>>() {

            @Override
            public void handle(Message<String> message) {
                    final String charBuffer = message.body();
                    getSharedMap();
                    boolean check = true;
                    for (Object obj : arrayOfPorts) {
                        if (((JsonObject)obj).getBoolean("reachable")) {
                            check = false;
                            ((JsonObject)obj).putBoolean("reachable", false);
                            setSharedMap();
                            final String remoteAddress = ((JsonObject)obj).getString("remoteAddress");
                            if (deploymentMap.get(remoteAddress) != null){
                                bus.send(remoteAddress, charBuffer);
                                bus.send("start.reading.data", "continue reading");
                            }else {
                                // Deploy for each MapReduce host one ReceiveData verticle to send data
                                container.deployWorkerVerticle("at.fhkaernten.collectSend.ReceiveData",
                                        new JsonObject("{\"port\":" + ((JsonObject) obj).getInteger("port") + "," +
                                                "\"ip\":\"" + ((JsonObject) obj).getString("ip") + "\"," +
                                                // RemoteAddress is used as an identifier
                                                "\"remoteAddress\":\"" + remoteAddress + "\"}"), 1, false,
                                        new AsyncResultHandler<String>() {

                                            @Override
                                            public void handle(AsyncResult<String> asyncResult) {
                                                if (asyncResult.succeeded()) {
                                                    bus.send(remoteAddress, charBuffer);
                                                    bus.send("start.reading.data", "continue reading");
                                                }
                                            } // handle
                                        });
                            }
                            break;
                        }
                    } //for
                    if (check){
                        bus.send("splitData.address", charBuffer);
                    }
            }//handle
        });

        bus.registerHandler("splitData.finish", new Handler<Message<String>>() {

            @Override
            public void handle(Message<String> message) {
                System.out.println("Finish");
            }
        });
    }

    private void initialize(){
        bus = vertx.eventBus();
        log = container.logger();
        arrayOfPorts = container.config().getArray("port_of_hosts");
        sharedMap = vertx.sharedData().getMap("pingMap");
        deploymentMap = new HashMap<>();
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
}

