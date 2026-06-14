package io.github.sunleader1997.reactorstream.abs;

import io.github.sunleader1997.reactorstream.abs.base.AbstractNode;
import io.github.sunleader1997.reactorstream.abs.base.DataSourceAbsNode;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

/**
 * 数据源监听模式
 * 预定好数据流
 * 当 {@link #emit(T item)} 时，触发数据流转
 * @param <T>
 */
public abstract class DataSourceByListen<T> extends AbstractNode<T> {

    public DataSourceByListen(WorkSpaceEnv workSpaceEnv) {
    }


    /**
     * 把数据推入流
     * @param item 单个数据
     */
    public void emit(T item) {
        Mono.just(item);
//        this.dataSink.emitNext(item, new Sinks.EmitFailureHandler() {
//            @Override
//            public boolean onEmitFailure(SignalType signalType, Sinks.EmitResult emitResult) {
//                try {
//                    Thread.sleep(100);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//                // 重试
//                System.out.printf("RETRY Emitting %s%n", item);
//                return true;
//            }
//        });
    }
}
