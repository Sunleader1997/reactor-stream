package io.github.sunleader1997.reactorstream.abs;

import io.github.sunleader1997.reactorstream.abs.base.DataSourceAbsNode;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * 数据源拉取模式
 * 线程会不停执行 {@link #pull()} 拉取数据
 * @param <T>
 */
public abstract class DataSourceByPull<T> extends DataSourceAbsNode<T> {

    /**
     * 拉取到的数据流
     */
    protected final Flux<T> dequeueFlux;

    protected DataSourceByPull(WorkSpaceEnv workSpaceEnv) {
        super(workSpaceEnv);
        this.dequeueFlux = Flux
                .defer(this::pull)
                .repeat()
                .subscribeOn(workSpaceEnv.getDequeueScheduler());
    }

    /**
     * 开始运行生产者
     */
    public abstract void startProducer() throws Exception;

    /**
     * 拉取数据实现, 后台会有单线程持续执行该任务
     */
    public abstract Publisher<T> pull();

    @Override
    public Flux<T> dequeueFlux() {
        return dequeueFlux;
    }
}
