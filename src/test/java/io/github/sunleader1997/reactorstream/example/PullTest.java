package io.github.sunleader1997.reactorstream.example;

import io.github.sunleader1997.reactorstream.abs.DataSourceByPull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import javax.security.auth.Destroyable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class PullTest {
    private static final AtomicInteger counter = new AtomicInteger(0);
    private static final Logger log = LoggerFactory.getLogger(PullTest.class);
    public static void main(String[] args) throws Exception {
        DataSourceByPull<Integer> dataSourceByPull = new DataSourceByPull<>() {

            @Override
            public Destroyable startProducer(){
                return null;
            }

            @Override
            protected List<Integer> fetchData() {
                List<Integer> list = new ArrayList<>();
                for (int i = 0; i < 10; i++) {
                    list.add(counter.incrementAndGet());
                }
                return list;
            }
        };
        dataSourceByPull.createConsumer(integerMono ->{
            return integerMono
                    .map(new StringToJson())
                    .doOnNext(json -> {
                        log.info(json);
                    });
        });
    }
}
