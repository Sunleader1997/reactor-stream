package io.github.sunleader1997.reactorstream.example;

import io.github.sunleader1997.reactorstream.abs.base.AbsPipeline;
import io.github.sunleader1997.reactorstream.abs.base.AbsDataSourceNode;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import javax.security.auth.Destroyable;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class ListenerTest {
    private static final AtomicInteger counter = new AtomicInteger(0);
    private static final Logger log = LoggerFactory.getLogger(ListenerTest.class);

    public static void main(String[] args) throws Exception {
        AbsDataSourceNode<String> listen = new AbsDataSourceNode<String>() {

            @Override
            public Destroyable startProducer(){
                Thread thread = new Thread(() -> {
                    while (true) {
                        int data = counter.get();
                        String item = "data|" + data;
                        log.info("produce -> {}",item);
                        Sinks.EmitResult result = this.tryEmit(item);
                        if (result.isSuccess()) {
                            counter.incrementAndGet();
                        }else{
                            log.error("Emit ERROR");
                        }
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

        AbsPipeline<String,String> mapPipeline = new AbsPipeline<String, String>() {
            @Override
            protected Function<Mono<String>, Publisher<String>> pipelines() {
                return mino-> mino
//                        .flatMapMany(item-> Flux.fromIterable(List.of("1>>>"+item,"2>>>"+item,"3>>>"+item)))
                        .flatMapMany(item-> Flux.fromIterable(List.of(">>>"+item)))
                        .doOnNext(item -> log.info("mapPipeline => {}",item));
            }

            @Override
            public void close() throws Exception {

            }
        };
        AbsPipeline<String,String> logPipeline = new AbsPipeline<String, String>() {
            @Override
            protected Function<Mono<String>, Publisher<String>> pipelines() {
                return mino-> mino
                        .doOnNext(item-> log.info("logPipeline1 => {}",item));
            }

            @Override
            public void close() throws Exception {

            }
        };
        AbsPipeline<String,String> logPipeline2 = new AbsPipeline<String, String>() {
            @Override
            protected Function<Mono<String>, Publisher<String>> pipelines() {
                return mino-> mino
                        .doOnNext(item-> log.info("logPipeline2 => {}",item));
            }

            @Override
            public void close() throws Exception {

            }
        };
        listen.outputTo(mapPipeline).outputTo(logPipeline);
        listen.outputTo(logPipeline2);
        // 初始化客户端/服务端
        listen.startProducerOnce();
    }
}
