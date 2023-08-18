package com.huberlin;

import com.huberlin.events.Event;
import org.apache.flink.api.common.functions.FilterFunction;

import java.util.HashSet;

public class DuplicateEventFilter implements FilterFunction<Event> {
    private final HashSet<String> seenEvents = new HashSet<>();

    @Override
    public boolean filter(Event value) {
        return seenEvents.add(value.getEventId());
    }
}
