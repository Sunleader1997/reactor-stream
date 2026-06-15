package io.github.sunleader1997.reactorstream.example;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reactor 示例：单线程拉取数据以及异步处理打印
 */
public class ReactorExample {

    private static final AtomicInteger counter = new AtomicInteger(0);

    public static void main(String[] args) throws InterruptedException {
        // 创建数据源 Sink
        Sinks.Many<String> dataSink = Sinks.many().multicast().onBackpressureBuffer(2, false);
        Flux<String> dataFlux = dataSink.asFlux();
        Scheduler single = Schedulers.newSingle("data-producer");
        Scheduler scheduler = Schedulers.newBoundedElastic(2,3,"proc");
        // 模拟单线程拉取数据：定时向 Sink 发送数据
        Flux.interval(Duration.ofMillis(100),single)
                .subscribe(tick -> {
                    String data = "data-" + counter.incrementAndGet();
                    System.out.println(new Date() + " >>> " + Thread.currentThread().getName() + ":" + data);
                    dataSink.emitNext(data, new Sinks.EmitFailureHandler() {
                        @Override
                        public boolean onEmitFailure(SignalType signalType, Sinks.EmitResult emitResult) {
                            System.out.printf("Emitting %s%n", Thread.currentThread().getName());
                            return false;
                        }
                    });
                });
        // 异步处理并打印：使用 subscribeOn 指定单线程拉取，publishOn 指定异步处理线程
        dataFlux
                .publishOn(Schedulers.newSingle("dequeue"))
                .flatMap(data -> asyncProcessor(data))
                .subscribe();

        // 主线程等待，观察异步处理效果
        Thread.sleep(50000);
        System.out.println("示例结束");
    }

    /**
     * 异步处理器：模拟耗时处理
     * @param str 输入数据
     * @return 处理结果
     */
    public static Mono<String> asyncProcessor(String str) {
        return Mono.fromCallable(() -> {
                    String result = "已处理: " + str.toUpperCase();
                    System.out.println(new Date() + " <<< " + Thread.currentThread().getName() + " : " + result);
                    // 模拟异步处理耗时
                    Thread.sleep(1000);
                    return result;
                });
    }

}
