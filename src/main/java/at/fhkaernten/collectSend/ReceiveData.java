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
 *  This class is used to send data.
 *  Will be deployed in CollectSend.java
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
        initialize();

        client = vertx.createNetClient();
        bus.registerHandler(remoteAddress, new Handler<Message<String>>() {

            @Override
            public void handle(final Message<String> message) {
                client.connect(portNumber, ip , new Handler<AsyncResult<NetSocket>>() {

                    @Override
                    public void handle(AsyncResult<NetSocket> event) {
                        if (event.succeeded()) {
                            socketToClose = event.result();
                            log.info("Connected to host " + host + " with " + portNumber + " and " + ip + " and ready to send data.");
                            // Split string text from UUID and write UUID to log file (info logging)
                            try{
                                container.logger().info("sendData:" + message.body().split("#ID#")[1] );
                            }catch (Exception e){
                                System.out.println(message.body());
                            }
                            event.result().write(message.body() + "#SOURCE#" + remoteAddress + "#TIME#" + System.currentTimeMillis() + "#UUID#" + message.body().split("#ID#")[1] +  "#END#");
                            event.result().close();
                            container.logger().info("sentData:" + message.body().split("#ID#")[1] );
                        } // if
                    } // handle
                });
            } // handle
        });
    }

    private void initialize(){
        log = container.logger();
        bus = vertx.eventBus();
        portNumber = container.config().getInteger("port");
        ip = container.config().getString("ip");
        host = container.config().getString("name");
        remoteAddress = container.config().getString("remoteAddress");
        portNumber++;
    }

    @Override
    public void stop(){
        if (socketToClose != null){
            try{
                socketToClose.close();
            } catch (Exception e){

            }
        }
        try {
            client.close();
        } finally {
            log.info("Stopping ReceiveData-Verticle.");
        }
    }
}
