package io.github.sunleader1997.reactorstream.example;

import io.github.sunleader1997.reactorstream.abs.DataSourceByPull;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;

public class ExampleMqDataSource extends DataSourceByPull<String> {

//    private KafkaConsumer<String, String> kafkaConsumer;

    @Override
    public void init() throws Exception {

    }

    @Override
    public void emit(String item) {

    }

    @Override
    public Flux<String> pull() {
//        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(500));
//        List<String> items = new ArrayList<>();
//        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
//            String recordTopic = consumerRecord.topic();
//            String item = consumerRecord.value();
//            items.add(item);
//        }
//        return Flux.fromIterable(items);
    }

    @Override
    public void destroy() throws Exception {

    }
}
