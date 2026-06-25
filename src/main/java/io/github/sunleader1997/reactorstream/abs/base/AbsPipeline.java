package io.github.sunleader1997.reactorstream.abs.base;

import io.github.sunleader1997.reactorstream.abs.BlockingEmitFailureHandler;
import io.github.sunleader1997.reactorstream.abs.PipelineManager;
import io.github.sunleader1997.reactorstream.abs.WorkSpaceEnv;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.*;
import java.util.function.Function;

/**
 * 管道
 * 接入上游数据，暴露输出
 */
public abstract class AbsPipeline<T, R> implements AutoCloseable {

    /**
     * 管道id
     */
    private final String id;
    /**
     * 收件箱
     */
    protected final Sinks.Many<T> receiver;
    protected final List<String> nextPipelineIds;
    protected Flux<R> flux;
    protected Disposable disposable;
    protected WorkSpaceEnv workSpaceEnv;

    public AbsPipeline(WorkSpaceEnv workSpaceEnv) {
        this(UUID.randomUUID().toString(), workSpaceEnv);
    }

    public AbsPipeline(String id, WorkSpaceEnv workSpaceEnv) {
        this.id = id;
        this.receiver = Sinks.many().multicast().onBackpressureBuffer();
        this.nextPipelineIds = new ArrayList<>();
        this.workSpaceEnv = workSpaceEnv;
        this.flux = receiver.asFlux()
                // 不定义背压会缓存 N 条数据后阻塞线程
                .flatMap(dataItem -> processors(dataItem))
                .doOnNext(out -> {
                    // 使用 BlockingEmitFailureHandler 让出空闲CPU，否则会因为 nextPipeline 的 receiver 被占满而阻塞
                    nextPipelineIds.forEach(nextPipelineId -> {
                        Optional<AbsPipeline<R, ?>> nextPipelineOptional = PipelineManager.get(nextPipelineId);
                        nextPipelineOptional.ifPresent(nextPipeline -> nextPipeline.emitBusyLooping(out));
                    });
                });
        this.disposable = this.flux.subscribe();
        PipelineManager.save(this);
    }

    public AbsPipeline<T, R> listen(AbsPipeline<?, T> absPipeline) {
        absPipeline.outputTo(this);
        return this;
    }

    public Sinks.EmitResult tryEmit(T item) {
        return receiver.tryEmitNext(item);
    }

    /**
     * 提交-阻塞
     */
    public void emitBusyLooping(T item) {
        receiver.emitNext(item, BlockingEmitFailureHandler.wait(Duration.ofMillis(10)));
    }

    /**
     * @param absPipelines 下一个节点
     * @return 下一个节点
     */
    @SafeVarargs
    public final <P> AbsPipeline<T, R> outputTo(AbsPipeline<R, P>... absPipelines) {
        for (AbsPipeline<R, P> absPipeline : absPipelines) {
            this.nextPipelineIds.add(absPipeline.getId());
        }
        return this;
    }

    /**
     * @param absPipelineIds 下一个节点Id
     * @return 下一个节点
     */
    public final AbsPipeline<T, R> outputTo(String... absPipelineIds) {
        this.nextPipelineIds.addAll(Arrays.asList(absPipelineIds));
        return this;
    }

    public Flux<R> processors(T dataItem) {
        Flux<R> rFlux = Mono.just(dataItem)
                .as(pipelines())
                .doOnNext(output -> onProcessSuccess(dataItem, output))
                .doOnError(error -> onProcessFailure(dataItem, error));
        if (workSpaceEnv.getConsumerSize() > 0) {
            return rFlux.publishOn(workSpaceEnv.getConsumerScheduler());
        }
        return rFlux;
    }

    /**
     * 从上游接收到数据时的处理
     * Mono 返回单个数据
     * Flux 返回多个数据
     */
    protected abstract Function<Mono<T>, Flux<R>> pipelines();

    /**
     * 单条数据处理成功回调
     *
     * @param input  输入数据
     * @param output 输出数据
     */
    protected void onProcessSuccess(T input, R output) {
    }

    /**
     * 单条数据处理失败回调
     *
     * @param input 输入数据
     * @param error 异常信息
     */
    protected void onProcessFailure(T input, Throwable error) {
    }

    public String getId() {
        return id;
    }

    @Override
    public void close() throws Exception {
        PipelineManager.remove(id);
        if (disposable != null) {
            this.disposable.dispose();
        }
    }
}
