package io.github.sunleader1997.reactorstream.example;

import io.github.sunleader1997.reactorstream.abs.base.DataSourceAbsNode;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.security.auth.Destroyable;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class ListenerTest {
    private static final AtomicInteger counter = new AtomicInteger(0);
    private static final Logger log = LoggerFactory.getLogger(ListenerTest.class);

    public static void main(String[] args) throws Exception {
        DataSourceAbsNode<String> listen = new DataSourceAbsNode<String>() {
            @Override
            protected Function<Mono<String>, Publisher<?>> pipelines() {
                return mino-> mino
                        .flatMapMany(item-> Flux.fromIterable(List.of(1,2,3)))
                        .doOnNext(item->{
                            log.info(item.toString());
                        });
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
            return stringMono.doOnNext(item->{
                log.info(">>> {}",item);
            });
        });
        // 初始化客户端/服务端
        listen.startProducerOnce();
    }
}
