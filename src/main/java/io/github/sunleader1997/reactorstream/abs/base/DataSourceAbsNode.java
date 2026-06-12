package io.github.sunleader1997.reactorstream.abs.base;


import io.github.sunleader1997.reactorstream.abs.DataSourceByPull;
import io.github.sunleader1997.reactorstream.abs.MapPipeline;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

/**
 * 数据源
 * @param <T>
 */
public abstract class DataSourceAbsNode<T> extends AbstractNode<T> {

    protected Flux<T> dataFlux;

    /**
     * 初始化
     */
    public abstract void init() throws Exception;

    /**
     * 手动提交数据给当前节点
     * @param item 单个数据
     */
    public abstract void emit(T item);

    /**
     * 向尾部增加订阅
     * @param pipeline 订阅
     */
    public DataSourceAbsNode<T> map(MapPipeline<T,?> pipeline) {
        this.dataFlux.map(pipeline);
        return this;
    }

    public void start(){
        this.dataFlux
                .subscribeOn(Schedulers.newSingle("dequeue"))
                .subscribe();;
    }
}
