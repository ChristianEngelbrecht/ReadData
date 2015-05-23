package at.fhkaernten.collectSend;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.net.NetClient;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.platform.Verticle;




/**
 * Created by Christian on 25.04.2015.
 */
public class ReceiveData extends Verticle {

    private EventBus bus;
    private NetClient client;
    private Logger log;
    private int portNumber;
    private String ip;
    private NetSocket socketToClose;
    private String host;
    private int check;
    private String remoteAddress;

    @Override
    public void start(){
        log = container.logger();
        bus = vertx.eventBus();
        portNumber = container.config().getInteger("port");
        ip = container.config().getString("ip");
        host = container.config().getString("name");
        remoteAddress = container.config().getString("remoteAddress");

        portNumber++;

        client = vertx.createNetClient();
        bus.registerHandler(remoteAddress, new Handler<Message<String>>() {
            @Override
            public void handle(final Message<String> message) {
                client.connect(portNumber, ip , new Handler<AsyncResult<NetSocket>>() {
                    @Override
                    public void handle(AsyncResult<NetSocket> event) {
                        if (event.succeeded()) {
                            socketToClose = event.result();
                            log.info("Connected to host " + host + " with " + portNumber + " and ready to send data.");
                            log.info(++check);
                            // Split string text from UUID and write UUID to log file (trace logging)
                            container.logger().trace("sendData:" + message.body().split("#ID#")[1] );
                            event.result().write(message.body() + "#SOURCE#" + remoteAddress + "#TIME#" + System.currentTimeMillis() + "#UUID#" + message.body().split("#ID#")[1] +  "#END#");
                            event.result().close();
                        }
                    }
                });
            }
        });
    }

    @Override
    public void stop(){
        if (socketToClose != null){
            try{
                socketToClose.close();
            } catch (Exception e){

            }
        } else {
            log.info("Stopping ReceiveData-Verticle.");
        }
        try {
            client.close();
        } finally {
            log.info("Stopping ReceiveData-Verticle.");
        }
    }
}
