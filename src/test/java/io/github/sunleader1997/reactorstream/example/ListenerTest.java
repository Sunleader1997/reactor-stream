package io.github.sunleader1997.reactorstream.example;

import io.github.sunleader1997.reactorstream.abs.DataSourceByListen;
import io.github.sunleader1997.reactorstream.abs.WorkSpaceEnv;
import reactor.core.publisher.Mono;

import javax.security.auth.DestroyFailedException;
import javax.security.auth.Destroyable;
import java.util.concurrent.atomic.AtomicInteger;

public class ListenerTest {
    private static final AtomicInteger counter = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {
        DataSourceByListen<String> listen = new DataSourceByListen<String>() {
            @Override
            public Destroyable startProducer(){
                Thread thread = new Thread(() -> {
                    while (true) {
                        int data = counter.incrementAndGet();
                        System.out.printf(Thread.currentThread().getName() + ": %d\n", data);
                        this.tryEmit("data|" + data);
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }, "producer");
                thread.start();
                return new Destroyable() {
                    @Override
                    public void destroy(){
                        thread.interrupt();
                    }
                };
            }
        };

        Thread.sleep(5000);
        listen.close();
    }
}
