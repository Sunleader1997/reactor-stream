package io.github.sunleader1997.reactorstream.abs.base;


import io.github.sunleader1997.reactorstream.abs.WorkSpaceEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import javax.security.auth.Destroyable;
import java.util.function.Function;

public abstract class AbsDataSourceNode<T> extends AbsPipeline<T,T> {

    private static final Logger log = LoggerFactory.getLogger(AbsDataSourceNode.class);
    protected Destroyable producerDisposable;
    protected volatile boolean producerStarted = false;
    protected volatile boolean destroyed = false;

    public AbsDataSourceNode() {
        this(new WorkSpaceEnv());
    }

    public AbsDataSourceNode(WorkSpaceEnv workSpaceEnv) {
        this.subscribeOn(workSpaceEnv);
    }

    @Override
    protected Function<Mono<T>, Flux<T>> pipelines() {
        return Mono::flux;
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
     * 返回发送结果，调用方自己决定重试/丢弃/记录
     */
    public Sinks.EmitResult tryEmit(T item) {
        return this.receiver.tryEmitNext(item);
    }

    @Override
    public synchronized void close() throws Exception {
        if (destroyed) {
            log.info("数据源已销毁，跳过重复销毁");
            return;
        }
        destroyed = true;
        log.info("数据源执行销毁");
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
        super.close();
    }
}
