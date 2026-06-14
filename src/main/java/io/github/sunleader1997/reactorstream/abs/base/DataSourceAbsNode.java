package io.github.sunleader1997.reactorstream.abs.base;


import io.github.sunleader1997.reactorstream.abs.WorkSpaceEnv;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

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
    public void createConsumer(Consumer<Flux<T>> publishOn){
        Flux<T> flux = this.dequeueFlux().publishOn(workSpaceEnv.getConsumerScheduler());
        publishOn.andThen(afterStreamCreated->{
            // 开启执行流水线
            Disposable disposable = afterStreamCreated.subscribe();
            // 流水线加入
            publishers.add(disposable);
        }).accept(flux);
    }

    public abstract Flux<T> dequeueFlux();
}
