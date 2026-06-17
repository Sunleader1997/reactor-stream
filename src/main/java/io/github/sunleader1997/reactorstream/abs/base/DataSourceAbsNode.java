package io.github.sunleader1997.reactorstream.abs.base;


import io.github.sunleader1997.reactorstream.abs.WorkSpaceEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import javax.security.auth.Destroyable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public abstract class DataSourceAbsNode<T> extends AbstractNode<T> {

    private static final Logger log = LoggerFactory.getLogger(DataSourceAbsNode.class);
    protected WorkSpaceEnv workSpaceEnv;
    protected final List<Disposable> publishers;
    protected Destroyable producerDisposable;
    protected volatile boolean producerStarted = false;
    protected volatile boolean destroyed = false;
    protected final Sinks.Many<T> dataSink;

    public DataSourceAbsNode() {
        this(new WorkSpaceEnv());
    }
    
    public DataSourceAbsNode(WorkSpaceEnv workSpaceEnv) {
        this.workSpaceEnv = workSpaceEnv;
        this.publishers = new ArrayList<>();
        this.dataSink = Sinks.many().multicast().onBackpressureBuffer();
        this.createConsumer(this.pipelines());
    }

    /**
     * 生产者定义
     * createConsumer 时自动执行
     * 可在内部创建客户端/服务端
     * 返回 Destroyable 用于自动销毁
     */
    protected Destroyable startProducer() {
        return null;
    }

    /**
     * 开始执行创建生产者
     */
    public synchronized void startProducerOnce() {
        if (!producerStarted) {
            this.producerDisposable = startProducer();
            producerStarted = true;
        }
    }

    /**
     * 实现，以在内部完成流程创建
     */
    protected Function<Mono<T>, Mono<?>> pipelines() {
        return null;
    }

    /**
     * 创建消费者
     * 执行多次创建多个消费者
     * 数据广播 到所有消费者
     */
    public synchronized void createConsumer(Function<Mono<T>, Mono<?>> tFunction) {
        if (tFunction == null) {
            return;
        }
        if (destroyed) {
            throw new IllegalStateException("DataSource has been destroyed, cannot create new consumer");
        }
        // 初始化客户端/服务端
        this.startProducerOnce();
        // 创建订阅着
        Disposable disposable = this.dequeueFlux()
                // 不定义背压会缓存 N 条数据后阻塞线程
                .flatMap(dataItem -> processors(dataItem, tFunction).subscribeOn(workSpaceEnv.getConsumerScheduler()), 10)
                .subscribe();
        // 流水线加入
        publishers.add(disposable);
    }

    protected Mono<?> processors(T dataItem, Function<Mono<T>, Mono<?>> tFunction) {
        return Mono.just(dataItem)
                .as(tFunction)
                .doOnSuccess(this::doOnSuccess)
                .onErrorContinue(this::onErrorContinue)
                .subscribeOn(workSpaceEnv.getConsumerScheduler());
    }
    /**
     * 返回发送结果，调用方自己决定重试/丢弃/记录
     */
    public Sinks.EmitResult tryEmit(T item) {
        return dataSink.tryEmitNext(item);
    }

    public Flux<T> dequeueFlux() {
        return dataSink.asFlux();
    }

    protected <R> void doOnSuccess(R item) {
        log.debug("doOnSuccess: {}", item);
    }

    protected <R> void onErrorContinue(Throwable throwable, R item) {
        log.error("onErrorContinue: {}", item, throwable);
    }

    @Override
    public synchronized void close() throws Exception {
        if (destroyed) {
            log.info("数据源已销毁，跳过重复销毁");
            return;
        }
        destroyed = true;
        log.info("数据源执行销毁");
        if (!publishers.isEmpty()) {
            for (Disposable disposable : publishers) {
                log.info("销毁消费者");
                try {
                    disposable.dispose();
                } catch (Exception e) {
                    log.warn(e.getMessage());
                }
            }
            publishers.clear();
        }
        if (producerDisposable != null) {
            try {
                log.info("销毁生产者");
                producerDisposable.destroy();
            } catch (Exception e) {
                log.warn("销毁失败", e);
            }
        }
        if (workSpaceEnv != null) {
            log.info("销毁线程池");
            workSpaceEnv.close();
        }
    }
}
