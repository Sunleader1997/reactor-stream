package io.github.sunleader1997.reactorstream.abs.base;


import io.github.sunleader1997.reactorstream.abs.WorkSpaceEnv;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public abstract class DataSourceAbsNode<T> extends AbstractNode<T> {

    protected WorkSpaceEnv workSpaceEnv;
    protected final List<Disposable> publishers;

    public DataSourceAbsNode(WorkSpaceEnv workSpaceEnv) {
        this.workSpaceEnv = workSpaceEnv;
        this.publishers = new ArrayList<>();
    }

    /**
     * 开始运行生产者
     */
    public abstract void startProducer() throws Exception;
    /**
     * 创建消费者
     */
    public void createConsumer(Function<Flux<T>,Flux<?>> publishOn){
        Flux<T> flux = this.dequeueFlux().publishOn(workSpaceEnv.getConsumerScheduler());
        Flux<?> afterStreamCreated = publishOn.apply(flux);
        // 开启执行流水线
        Disposable disposable = afterStreamCreated.subscribe();
        // 流水线加入
        publishers.add(disposable);
    }

    public abstract Flux<T> dequeueFlux();
}
