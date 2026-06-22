package io.github.sunleader1997.reactorstream.example;

import io.github.sunleader1997.reactorstream.abs.WorkSpaceEnv;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class ListenerTest {
    private static final Long MAX_NUMBER = 100000L; // 10w
    private static final AtomicInteger counter = new AtomicInteger(0);
    private static final Logger log = LoggerFactory.getLogger(ListenerTest.class);

    public static void main(String[] args) throws Exception {
        AbsDataSourceNode<String> listen = new AbsDataSourceNode<>(new WorkSpaceEnv(true)) {

            @Override
            public Destroyable startProducer() {
                Thread thread = new Thread(() -> {
                    long start = System.currentTimeMillis();
                    while (counter.get() <= MAX_NUMBER) {
                        int data = counter.get();
                        String item = "data|" + data;
//                        log.info("produce -> {}",item);
                        this.emitBusyLooping(item);
                        counter.incrementAndGet();
                    }
                    long end = System.currentTimeMillis();
                    log.info("tps => {}/s", MAX_NUMBER * 1000 / (end - start));
                }, "producer");
                thread.start();
                return new Destroyable() {
                    @Override
                    public void destroy() {
                        thread.interrupt();
                    }
                };
            }
        };
        AbsPipeline<String,String> mapPipeline = new AbsPipeline<String, String>() {
            @Override
            protected Function<Mono<String>, Flux<String>> pipelines() {
                return mino-> mino
//                        .flatMapMany(item-> Flux.fromIterable(List.of("1>>>"+item,"2>>>"+item,"3>>>"+item)))
                        .flatMapMany(item-> Flux.fromIterable(List.of(">>>"+item)))
                        .doOnNext(item -> {
                            //log.info("mapPipeline => {}",item);
                        });
            }
        };
        AbsPipeline<String,String> logPipeline = new AbsPipeline<String, String>() {
            @Override
            protected Function<Mono<String>, Flux<String>> pipelines() {
                return mino-> mino
                        .doOnNext(item-> {
                            //log.info("logPipeline1 => {}",item);
                        })
                        .flux();
            }
        };
        AtomicLong count = new AtomicLong(0);
        long start = System.currentTimeMillis();
        AbsPipeline<String, String> logPipeline2 = new AbsPipeline<String, String>() {
            @Override
            protected Function<Mono<String>, Flux<String>> pipelines() {
                return mino -> mino
                        .doOnNext(item -> {
                            long current = count.getAndIncrement();
                            if (current >= MAX_NUMBER) {
                                log.info("logPipeline2 => {}",current);
                                long end = System.currentTimeMillis();
                                log.info("logPipeline2 tpc => {}/s", MAX_NUMBER * 1000 / (end - start));
                            }
                        }).flux();
            }
        };
        //listen.outputTo(mapPipeline).outputTo(logPipeline);
        listen.outputTo(logPipeline2);
        // 初始化客户端/服务端
        listen.startProducerOnce();
    }
}
