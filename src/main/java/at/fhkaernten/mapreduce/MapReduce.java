package at.fhkaernten.mapreduce;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.platform.Verticle;

/**
 * Created by Christian on 08.03.2015.
 */
public class MapReduce extends Verticle {

    @Override
    public void start(){
        int test = container.config().getInteger("Test");
        System.out.print("Test" + test);
        Handler<Message<String>> h1 = new Handler<Message<String>>() {
            @Override
            public void handle(Message<String> o) {
                System.out.print(o.body());
            }
        };
        EventBus bus = vertx.eventBus();
        bus.registerHandler("test" , h1);


    }
}
