package io.github.sunleader1997.reactorstream.abs;

import io.github.sunleader1997.reactorstream.abs.base.DataSourceAbsNode;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger log = LoggerFactory.getLogger(DataSourceByPull.class);

    public DataSourceByPull(){
        this(new WorkSpaceEnv());
    }

    public DataSourceByPull(String name){
        this(new WorkSpaceEnv(name));
    }
    /**
     * 拉取到的数据流
     */
    protected final Flux<T> dequeueFlux;

    protected DataSourceByPull(WorkSpaceEnv workSpaceEnv) {
        super(workSpaceEnv);
        Duration waitDuration = workSpaceEnv.getWaitDuration();
        log.info("DataSourceByPull initialized, dequeueScheduler={}, waitDuration={}",
                workSpaceEnv.getDequeueScheduler().getClass().getSimpleName(), waitDuration);
        this.dequeueFlux = Flux
                .defer(this::pull)
                .repeat()
                .subscribeOn(workSpaceEnv.getDequeueScheduler())
                .share();
    }

    /**
     * 拉取数据实现, 后台会有单线程持续执行该任务
     * 空数据时通过 Mono.delay 非阻塞等待
     */
    public Publisher<T> pull() {
        List<T> dataList = this.fetchData();
        if (dataList == null || dataList.isEmpty()) {
            Duration waitDuration = workSpaceEnv.getWaitDuration();
            log.info("fetchData returned empty, wait {} before next pull", waitDuration);
            // Mono.delay(duration) 不指定 Scheduler 时默认用 Schedulers.parallel()，延时完成后 .repeat() 的下一个元素就在 parallel 线程上发出了
            return Mono.delay(waitDuration, workSpaceEnv.getDequeueScheduler()).then(Mono.empty());
        }
        log.info("fetchData returned {} items", dataList.size());
        return Flux.fromIterable(dataList);
    }

    protected abstract List<T> fetchData();

    @Override
    public Flux<T> dequeueFlux() {
        return dequeueFlux;
    }
}
