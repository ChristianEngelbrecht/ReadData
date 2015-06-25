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
 * Class PingVerticle is used to check initially if MapReduce hosts are reachable and free to process data
 */
public class PingVerticle extends Verticle{
    private Logger log;
    private EventBus bus;
    private NetClient client;
    private JsonArray arrayOfPorts;
    private NetSocket socketToClose;
    private NetServer server;
    /* ConcurrentMap is a sharedMap (function in Vert.x) which can be accessed by all verticle instances to exchange data */
    private ConcurrentMap<String, String> sharedMap;

    @Override
    public void start(){
        initialize();
        vertx.setPeriodic(1000 * 5, new Handler<Long>() {
            @Override
            public void handle(Long event) {
                // This NetClient is used to ping all available MapReduce hosts to check their availability
                client = vertx.createNetClient();
                for (Object host : arrayOfPorts) {
                    final int port = ((JsonObject) host).getInteger("port");
                    final String ip = ((JsonObject) host).getString("ip");

                    client.connect(port, ip , new AsyncResultHandler<NetSocket>() {

                        @Override
                        public void handle(AsyncResult<NetSocket> event) {
                            if (event.succeeded()) {
                                socketToClose = event.result();
                                final NetSocket socket = event.result();
                                socket.dataHandler(new Handler<Buffer>() {

                                    @Override
                                    public void handle(Buffer buffer) {
                                        for (Object obj : arrayOfPorts) {
                                            int portTmp = ((JsonObject) obj).getInteger("port");
                                            if (portTmp == Integer.valueOf(buffer.toString())) {
                                                ((JsonObject) obj).putBoolean("reachable", true);
                                                setSharedMap();
                                                log.info(String.format("Ping %s @Port %s @ip %s was successful.", ((JsonObject) obj).getString("name"), ((JsonObject) obj).getInteger("port"), ((JsonObject) obj).getString("ip")));
                                                socket.close();
                                                break;
                                            }
                                        }
                                    }
                                });
                                socket.write(new Buffer("ping"));
                            }
                        }
                    });
                }
            }
        });
        // This NetServer is responsible to receive replies from free (free = no operation) MapReduce hosts
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
        }
        try {
            try {
                server.close();
            }catch(Exception e){}
            client.close();
        } finally {
            log.info("Stopping PingVerticle.");
        }
    }
}
