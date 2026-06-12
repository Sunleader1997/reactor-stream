package io.github.sunleader1997.reactorstream;

import io.github.sunleader1997.reactorstream.abs.base.DataSourceAbsNode;

public class DataSourceFactory {
    private static final int AVAILABLE_PROS = Runtime.getRuntime().availableProcessors();

    /**
     * 创建数据流实例
     * @param processThreads 处理线程数量
     */
    public static <T extends DataSourceAbsNode<?>> T create(Class<T> dataSource,int processThreads) {
        // TODO CREATE BY CLASS
        return null;
    }

    /**
     * 创建数据流实例
     */
    public static <T extends DataSourceAbsNode<?>> T create(Class<T> dataSource) {
        return create(dataSource,AVAILABLE_PROS);
    }

}
