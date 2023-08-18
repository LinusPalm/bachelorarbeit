package com.huberlin;

import com.huberlin.events.Event;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class PrintProcessFunction extends ProcessFunction<Event, Event> {
    private final String prefix;

    public PrintProcessFunction(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public void processElement(Event value, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) throws Exception {
        System.out.println(this.prefix + value.toString());

        out.collect(value);
    }
}
