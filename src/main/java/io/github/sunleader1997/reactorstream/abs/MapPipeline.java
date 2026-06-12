package io.github.sunleader1997.reactorstream.abs;

import io.github.sunleader1997.reactorstream.abs.base.PipelineAbsNode;

import java.util.function.Function;

/**
 * 数据转换
 * @param <T>
 * @param <R>
 */
public abstract class MapPipeline<T,R> extends PipelineAbsNode<T> implements Function<T, R> {
}
