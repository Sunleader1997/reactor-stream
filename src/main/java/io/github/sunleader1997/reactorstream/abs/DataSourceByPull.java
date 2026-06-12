package io.github.sunleader1997.reactorstream.abs;

import io.github.sunleader1997.reactorstream.abs.base.DataSourceAbsNode;
import reactor.core.publisher.Flux;

public abstract class DataSourceByPull<T> extends DataSourceAbsNode<T> {

    public DataSourceByPull() {
        Flux<T> dequeue = Flux.defer(this::pull).repeat();
        this.dataFlux = Flux.from(dequeue);
    }


    /**
     * Pull 模式
     * 拉取数据
     */
    public abstract Flux<T> pull();

}
