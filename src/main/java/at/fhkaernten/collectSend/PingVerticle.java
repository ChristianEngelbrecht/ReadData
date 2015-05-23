package at.fhkaernten.collectSend;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.net.NetClient;
import org.vertx.java.core.net.NetServer;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.platform.Verticle;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by Christian on 12.04.2015.
 */
public class PingVerticle extends Verticle{
    private Logger log;
    private EventBus bus;
    private NetClient client;
    private JsonArray arrayOfPorts;
    private NetSocket socketToClose;
    private NetServer server;
    //shared Data
    private ConcurrentMap<String, String> sharedMap;
    @Override
    public void start(){
        initialize();

        vertx.setPeriodic(1000 * 5, new Handler<Long>() {
            @Override
            public void handle(Long event) {
                // Dieser NetServer pingt am Anfang alle Hosts.
                client = vertx.createNetClient();
                for (Object host : arrayOfPorts) {
                    final int port = ((JsonObject) host).getInteger("port");
                    final String ip = ((JsonObject) host).getString("ip");

                    client.connect(port, ip , new AsyncResultHandler<NetSocket>() {
                        //Event.Succeeded trifft dann zu, wenn sich der Client erfolgreich mit dem MapReduce Modul verbinden konnte
                        // Erst wenn ein Event zutrifft, springt der Handler an und reagiert
                        // Es trifft immer ein Event ein (false oder true) -> Bei true wird wieder ein neuer Socket mitgegeben!
                        @Override
                        public void handle(AsyncResult<NetSocket> event) {
                            if (event.succeeded()) {
                                socketToClose = event.result();
                                final NetSocket socket = event.result();
                                socket.dataHandler(new Handler<Buffer>() {
                                    @Override
                                    public void handle(Buffer buffer) { // Hier wird die Portnummer des freien Hosts ausgelesen
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
                                socket.write(new Buffer("ping"));
                            }
                        }
                    });
                }
            }
        });

        server = vertx.createNetServer();
        server.connectHandler(new Handler<NetSocket>() {
            @Override
            public void handle(NetSocket event){
                final NetSocket socket = event;
                socket.dataHandler(new Handler<Buffer>() {
                    @Override
                    public void handle(Buffer buffer) {
                        for (Object obj : arrayOfPorts) {
                            int portTmp = ((JsonObject) obj).getInteger("port");
                            if (portTmp == Integer.valueOf(buffer.toString())) {
                                ((JsonObject) obj).putBoolean("reachable", true);
                                setSharedMap();
                                //log.info(String.format("Ping %s @Port %s was successful.", ((JsonObject) obj).getString("name"), ((JsonObject) obj).getInteger("port")));
                                socket.close();
                                break;
                            }
                        }
                    }
                });
            }
        }).listen(667);

    }

    private void initialize(){
        log = container.logger();
        bus = vertx.eventBus();
        arrayOfPorts =  container.config().getArray("port_of_hosts");
        sharedMap = vertx.sharedData().getMap("pingMap");
        //initialize shared Map
        setSharedMap();
    }

    //overwrite shared map
    private void setSharedMap(){
                for (Object obj : arrayOfPorts){
                    String hostName = ((JsonObject) obj).getString("name");
            sharedMap.put(hostName, ((JsonObject) obj).encode());
        }
    }

    @Override
    public void stop(){
        if (socketToClose != null){
            try{
                socketToClose.close();
            } catch (Exception e){}
        } else {
            log.info("Stopping ReduceSend-Verticle.");
        }
        try {
            try {
                server.close();
            }catch(Exception e){}
            client.close();
        } finally {
            log.info("Stopping ReduceSend-Verticle.");
        }
    }
}
