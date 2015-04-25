package at.fhkaernten.collectSend;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.net.NetClient;
import org.vertx.java.core.net.NetServer;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.platform.Verticle;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by Christian on 12.04.2015.
 */
public class PingVerticle extends Verticle{
    private Logger log;
    private EventBus bus;
    private NetClient client;
    private JsonArray arrayOfPorts;
    //shared Data
    private ConcurrentMap<String, String> sharedMap;
    @Override
    public void start(){
        log = container.logger();
        bus = vertx.eventBus();
        arrayOfPorts =  container.config().getArray("Port_Of_Hosts");
        sharedMap = vertx.sharedData().getMap("pingMap");
        //initialize shared Map
        setSharedMap();


        vertx.setPeriodic(1000 * 2, new Handler<Long>() {
            @Override
            public void handle(Long event) {
            // Dieser NetServer pingt am Anfang alle Hosts.
            client = vertx.createNetClient();
            //connection timeout = 5 seconds
            //client.setConnectTimeout(1000*5);
            for (Object host : arrayOfPorts) {
                final int port = ((JsonObject) host).getInteger("port");
                final String name = ((JsonObject) host).getString("name");
                client.connect(port, "localhost", new AsyncResultHandler<NetSocket>() {
                    //Event.Succeeded trifft dann zu, wenn sich der Client erfolgreich mit dem MapReduce Modul verbinden konntte
                    // Erst wenn ein Event zutrifft, springt der Handler an und reagiert
                    // Es trifft immer ein Event ein (false oder true) -> Bei true wird wieder ein neuer Socket mitgegeben!
                    @Override
                    public void handle(AsyncResult<NetSocket> event) {
                    if (event.succeeded()) {
                        final NetSocket socket = event.result();
                        socket.dataHandler(new Handler<Buffer>() {
                            @Override
                            public void handle(Buffer buffer) { // Hier wird die Portnummer des freien Hosts ausgelesen
                                System.out.print(buffer.toString());
                            for (Object obj : arrayOfPorts) {
                                int portTmp = ((JsonObject) obj).getInteger("port");
                                if (portTmp == Integer.valueOf(buffer.toString())) {
                                    ((JsonObject) obj).putBoolean("reachable", true);
                                    setSharedMap();
                                    log.info(String.format("Ping %s @Port %s was successful.", ((JsonObject) obj).getString("name"), ((JsonObject) obj).getInteger("port")));
                                    socket.close();
                                    break;
                                }
                            }
                            }
                        });
                        // now send some data
                        log.info(String.format("Ping %s @Port %s", name, port));
                        socket.write(new Buffer("ping"));
                    }
                    }
                });
            }
            }
        });

    }
    //overwrite shared map
    private void setSharedMap(){
        for (Object obj : arrayOfPorts){
            String hostName = ((JsonObject) obj).getString("name");
            sharedMap.put(hostName, ((JsonObject) obj).encode());
        }
    }
}
