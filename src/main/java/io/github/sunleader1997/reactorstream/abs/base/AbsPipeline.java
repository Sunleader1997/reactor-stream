package io.github.sunleader1997.reactorstream.abs.base;

import io.github.sunleader1997.reactorstream.abs.BlockingEmitFailureHandler;
import io.github.sunleader1997.reactorstream.abs.WorkSpaceEnv;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.annotation.NonNull;

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
    protected Flux<R> flux;
    protected Disposable disposable;
    protected WorkSpaceEnv workSpaceEnv;
    protected volatile boolean initialized = false;

    public AbsPipeline() {
        this.receiver = Sinks.many().multicast().onBackpressureBuffer();
        this.nextPipelines = new ArrayList<>();
    }

    public AbsPipeline<T,R> listen(AbsPipeline<?,T> absPipeline) {
        absPipeline.outputTo(this);
        return this;
    }

    /**
     * 设置订阅线程池
     */
    public void trySetupPipeline(@NonNull WorkSpaceEnv workSpaceEnv) {
        if (initialized) return;
        synchronized (this) {
            if (!initialized) {  // double-check
                this.workSpaceEnv = workSpaceEnv;
                this.flux = receiver.asFlux()
                        // 不定义背压会缓存 N 条数据后阻塞线程
                        .flatMap(dataItem -> processors(dataItem).subscribeOn(workSpaceEnv.getConsumerScheduler()))
                        .doOnNext(out->{
                            nextPipelines.forEach(nextPipeline->nextPipeline.emitBusyLooping(out));
                        });
                this.disposable = this.flux.subscribe();
                this.initialized = true;
            }
        }
    }
    /**
     * 提交-阻塞
     */
    public void emitBusyLooping(T item) {
        receiver.emitNext(item, BlockingEmitFailureHandler.wait(Duration.ofSeconds(1)));
    }

    /**
     * @param absPipeline 下一个节点
     * @return 下一个节点
     */
    public <P> AbsPipeline<R,P> outputTo(AbsPipeline<R,P> absPipeline) {
        absPipeline.trySetupPipeline(workSpaceEnv);
        this.nextPipelines.add(absPipeline);
        return absPipeline;
    }

    protected Flux<R> processors(T dataItem) {
        return Mono.just(dataItem)
                .as(pipelines());
    }

    /**
     * 从上游接收到数据时的处理
     * Mono 返回单个数据
     * Flux 返回多个数据
     */
    protected abstract Function<Mono<T>, Flux<R>> pipelines();

    @Override
    public void close() throws Exception {
        if (disposable != null) {
            this.disposable.dispose();
        }
    }
}
