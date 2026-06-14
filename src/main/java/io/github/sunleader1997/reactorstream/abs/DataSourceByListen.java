package io.github.sunleader1997.reactorstream.abs;

import io.github.sunleader1997.reactorstream.abs.base.DataSourceAbsNode;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

/**
 * 数据源监听模式
 * 预定好数据流
 * 当 {@link #tryEmit(T item)} 时，触发数据流转
 *
 * @param <T>
 */
public abstract class DataSourceByListen<T> extends DataSourceAbsNode<T> {

    private final Sinks.Many<T> dataSink;

    public DataSourceByListen(WorkSpaceEnv workSpaceEnv) {
        super(workSpaceEnv);
        this.dataSink = Sinks.many().multicast().onBackpressureBuffer();
    }

    /**
     * 返回发送结果，调用方自己决定重试/丢弃/记录
     */
    public Sinks.EmitResult tryEmit(T item) {
        return dataSink.tryEmitNext(item);
    }

    @Override
    public Flux<T> dequeueFlux() {
        return dataSink.asFlux();
    }
}
