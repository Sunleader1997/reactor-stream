package io.github.sunleader1997.reactorstream.abs.base;


import io.github.sunleader1997.reactorstream.abs.WorkSpaceEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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

    public DataSourceAbsNode(WorkSpaceEnv workSpaceEnv) {
        this.workSpaceEnv = workSpaceEnv;
        this.publishers = new ArrayList<>();
    }

    /**
     * 生产者定义
     * 返回 Destroyable 用于自动销毁
     */
    protected abstract Destroyable startProducer();

    /**
     * 开始执行创建生产者
     */
    public synchronized void startProducerOnce() {
        if (!producerStarted) {
            this.producerDisposable = startProducer();
            producerStarted = true;
        }
    }

//    /**
//     * 创建消费者
//     * 执行多次创建多个消费者
//     * 数据广播 到所有消费者
//     */
//    public synchronized void createConsumer(Function<Flux<T>, Flux<?>> publishOn) {
//        if (destroyed) {
//            throw new IllegalStateException("DataSource has been destroyed, cannot create new consumer");
//        }
//        // 初始化生产
//        this.startProducerOnce();
//        // 创建订阅着
//        Flux<T> flux = this.dequeueFlux().publishOn(workSpaceEnv.getConsumerScheduler());
//        Flux<?> afterStreamCreated = publishOn.apply(flux);
//        // 开启执行流水线
//        Disposable disposable = afterStreamCreated.subscribe();
//        // 流水线加入
//        publishers.add(disposable);
//    }

    /**
     * 创建消费者
     * 执行多次创建多个消费者
     * 数据广播 到所有消费者
     */
    public synchronized void createConsumer(Function<T, ?> tFunction) {
        if (destroyed) {
            throw new IllegalStateException("DataSource has been destroyed, cannot create new consumer");
        }
        // 创建订阅着
        Disposable disposable = this.dequeueFlux()
                // 不定义背压会缓存 N 条数据后阻塞线程
                .flatMap(dataItem -> processors(dataItem, tFunction).subscribeOn(workSpaceEnv.getConsumerScheduler()), 10)
                .subscribe();
        // 流水线加入
        publishers.add(disposable);
    }

    public <R> Mono<R> processors(T dataItem, Function<T, R> tFunction) {
        return Mono.just(dataItem)
                .map(tFunction)
                .doOnSuccess(this::doOnSuccess)
                .onErrorContinue(this::onErrorContinue)
                .subscribeOn(workSpaceEnv.getConsumerScheduler());
    }

    public abstract Flux<T> dequeueFlux();

    public <R> void doOnSuccess(R item) {
        log.debug("doOnSuccess: {}", item);
    }

    public <R> void onErrorContinue(Throwable throwable, R item) {
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
