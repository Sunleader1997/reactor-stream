package io.github.sunleader1997.reactorstream.abs;

import io.github.sunleader1997.reactorstream.abs.base.DataSourceAbsNode;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

/**
 * 数据源拉取模式
 * 线程会不停执行 {@link #pull()} 拉取数据
 *
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
     * 拉取数据实现, 后台会有单线程持续执行该任务
     * 空数据时通过 Mono.delay 非阻塞等待
     */
    public Publisher<T> pull() {
        List<T> dataList = this.fetchData();
        if (dataList == null || dataList.isEmpty()) {
            return Mono.delay(workSpaceEnv.getWaitDuration()).then(Mono.empty());
        }
        return Flux.fromIterable(dataList);
    }

    public abstract List<T> fetchData();

    @Override
    public Flux<T> dequeueFlux() {
        return dequeueFlux;
    }
}
