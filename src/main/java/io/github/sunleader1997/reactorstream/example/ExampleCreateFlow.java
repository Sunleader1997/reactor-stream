package io.github.sunleader1997.reactorstream.example;

import io.github.sunleader1997.reactorstream.DataSourceFactory;
import io.github.sunleader1997.reactorstream.abs.MapPipeline;

/**
 * 演示如何创建一个完整的数据流
 */
public class ExampleCreateFlow {

    public static void main(String[] args) {
        DataSourceFactory.create(ExampleMqDataSource.class)
                .map(new MapPipeline<String, String>() {
                    @Override
                    public String apply(String t) {
                        return t;
                    }

                    @Override
                    public void destroy() throws Exception {

                    }
                })
                .start();
    }
}
