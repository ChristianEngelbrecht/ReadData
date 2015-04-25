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

    @Override
    public void start(){
        log = container.logger();
        bus = vertx.eventBus();
        portNumber = container.config().getInteger("port");
        client = vertx.createNetClient();
        bus.registerHandler(container.config().getString("remoteAddress"), new Handler<Message<String>>() {
            @Override
            public void handle(final Message<String> message) {
                client.connect(++portNumber, "localhost", new Handler<AsyncResult<NetSocket>>() {
                    @Override
                    public void handle(AsyncResult<NetSocket> event) {
                        if (event.succeeded()) {
                            log.info("Connected to host " + container.config().getString("name") + " with " + container.config().getInteger("port") + " and ready to send data.");
                            event.result().write(message.body());
                            event.result().close();
                            bus.send("finish", container.config().getString("remoteAddress"));
                        }
                    }
                });
            }
        });
    }
}
