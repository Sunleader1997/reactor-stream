package io.github.sunleader1997.reactorstream.example;


import java.util.function.Function;

public class StringToJson2 implements Function<String, String> {
    @Override
    public String apply(String string) {
        System.out.println("string to json");
        return string;
    }
}
