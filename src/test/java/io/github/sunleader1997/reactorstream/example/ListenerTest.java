package io.github.sunleader1997.reactorstream.example;

import io.github.sunleader1997.reactorstream.abs.base.DataSourceAbsNode;
import reactor.core.publisher.Mono;

import javax.security.auth.Destroyable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class ListenerTest {
    private static final AtomicInteger counter = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {
        DataSourceAbsNode<String> listen = new DataSourceAbsNode<String>() {
            @Override
            protected Function<Mono<String>, Mono<?>> pipelines() {
                return mino-> mino
                        .doOnNext(System.out::println);
            }

            @Override
            public Destroyable startProducer(){
                Thread thread = new Thread(() -> {
                    while (true) {
                        int data = counter.incrementAndGet();
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
        listen.createConsumer(stringMono -> {
            return stringMono.doOnNext(System.out::println);
        });

        Thread.sleep(5000);
        listen.close();
    }
}
