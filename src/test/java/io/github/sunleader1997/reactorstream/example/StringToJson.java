package io.github.sunleader1997.reactorstream.example;

import java.util.function.Function;

public class StringToJson implements Function<Integer, String> {
    @Override
    public String apply(Integer intValue) {
        System.out.println("string to json");
        return intValue.toString();
    }
}
