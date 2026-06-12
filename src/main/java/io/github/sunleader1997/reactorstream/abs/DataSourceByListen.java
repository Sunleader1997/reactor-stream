package io.github.sunleader1997.reactorstream.abs;

import io.github.sunleader1997.reactorstream.abs.base.DataSourceAbsNode;
import reactor.core.publisher.Sinks;

public abstract class DataSourceByListen<T> extends DataSourceAbsNode<T> {

    protected Sinks.Many<T> dataSink;

    public DataSourceByListen() {
        this.dataSink = Sinks.many().multicast().onBackpressureBuffer(256, false);
        this.dataFlux = this.dataSink.asFlux();
    }

    @Override
    public void emit(T item) {
        this.dataSink.tryEmitNext(item);
    }
}
