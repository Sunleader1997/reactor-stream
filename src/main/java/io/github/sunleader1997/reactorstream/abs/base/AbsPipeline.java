package io.github.sunleader1997.reactorstream.abs.base;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * 管道
 * 接入上游数据，暴露输出
 */
public abstract class AbsPipeline<T,R> implements AutoCloseable {

    protected Flux<R> flux;

    public void dataFrom(AbsPipeline<?,T> absPipeline) {
        if (this.flux != null) {
            throw new IllegalStateException("Flux already initialized");
        }
        this.flux = absPipeline.getFlux()
                // 不定义背压会缓存 N 条数据后阻塞线程
                .flatMap(dataItem -> processors(dataItem))
                // 使用 share() 使 flux 成为可共享的热发布者
                // 避免多个下游订阅者重复触发上游处理逻辑
                .share();
        this.flux.subscribe();
    }

    protected Publisher<R> processors(T dataItem) {
        return Mono.just(dataItem)
                .as(pipelines());
    }

    /**
     * 从上游接收到数据时的处理
     * Mono 返回单个数据
     * Flux 返回多个数据
     */
    protected abstract Function<Mono<T>, Publisher<R>> pipelines();

    /**
     * 连接管道
     * 将数据输出给管道
     */
    public void outputTo(AbsPipeline<R,?> absPipeline){
        absPipeline.dataFrom(this);
    }

    public Flux<R> getFlux() {
        return flux;
    }
}
