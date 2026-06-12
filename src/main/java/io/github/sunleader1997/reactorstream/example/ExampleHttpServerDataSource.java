package io.github.sunleader1997.reactorstream.example;

import io.github.sunleader1997.reactorstream.abs.DataSourceByListen;

import java.util.Map;
import java.util.function.Consumer;

public class ExampleHttpServerDataSource extends DataSourceByListen<Map<String,Object>> {

    private ExampleHttpServer server;

    /**
     * create server
     * then use "emit" to push data
     *
     * @throws Exception server init failed
     */
    @Override
    public void init() throws Exception {

    }

    @Override
    public void emit(Map<String,Object> item) {
        this.push(item);
    }

    /**
     * destroy the server
     * @throws Exception clean failed
     */
    @Override
    public void destroy() throws Exception {
        if (server != null) {
            server.dispose();
        }
    }

    public static class ExampleHttpServer{


        public static ExampleHttpServer create(){
            return new ExampleHttpServer();
        }

        public ExampleHttpServer port(int port){
            return this;
        }

        public ExampleHttpServer route(Consumer<Map<String,Object>> object) {
            return this;
        }

        public ExampleHttpServer bindNow() {
            return this;
        }

        public void dispose() {
        }
    }
}
