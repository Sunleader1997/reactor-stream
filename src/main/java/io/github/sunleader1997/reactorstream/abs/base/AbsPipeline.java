package io.github.sunleader1997.reactorstream.abs.base;

import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * 管道
 * 接入上游数据，暴露输出
 */
public abstract class AbsPipeline<T,R> implements AutoCloseable {

    /**
     * 收件箱
     */
    protected final Sinks.Many<T> receiver;
    protected final List<AbsPipeline<R,?>> nextPipelines;
    protected final Flux<R> flux;
    protected final Disposable disposable;

    public AbsPipeline() {
        this.receiver = Sinks.many().multicast().onBackpressureBuffer();
        this.nextPipelines = new ArrayList<>();
        this.flux = receiver.asFlux()
                // 不定义背压会缓存 N 条数据后阻塞线程
                .flatMap(dataItem -> processors(dataItem))
                .doOnNext(out->{
                    nextPipelines.forEach(nextPipeline->nextPipeline.emitBusyLooping(out));
                });
        this.disposable = this.flux.subscribe();
    }

    public AbsPipeline<T,R> listen(AbsPipeline<?,T> absPipeline) {
        absPipeline.outputTo(this);
        return this;
    }

    /**
     * 提交-阻塞
     */
    public void emitBusyLooping(T item) {
        receiver.emitNext(item,Sinks.EmitFailureHandler.busyLooping(Duration.ofSeconds(1)));
    }

    /**
     * @param absPipeline 下一个节点
     * @return 下一个节点
     */
    public <P> AbsPipeline<R,P> outputTo(AbsPipeline<R,P> absPipeline) {
        this.nextPipelines.add(absPipeline);
        return absPipeline;
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

}
