package com.huberlin.events;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

public class PrimitiveEvent extends Event {
    public PrimitiveEvent() {
        super();
    }

    public PrimitiveEvent(String eventType, String eventId, Instant timestamp, String sourceNodeId) {
        super(eventType, eventId, timestamp, sourceNodeId);
    }

    @Override
    public long getHighestTimestamp() {
        return this.getTimestampMs();
    }

    @Override
    public long getLowestTimestamp() {
        return this.getTimestampMs();
    }

    @Override
    public List<PrimitiveEvent> deconstruct() {
        return Collections.singletonList(this);
    }

    @Override
    public String toString() {
        return String.join(" | ",
                "simple",
                this.getEventId(),
                this.getTimestamp().toString(),
                this.getEventType());
    }

    @Override
    public String getEventIdByEventType(String eventType) {
        if (eventType.equals(getEventType())) {
            return getEventId();
        }

        return null;
    }

    @Override
    public long getTimestampByEventType(String eventType) {
        if (eventType.equals(getEventType())) {
            return getTimestampMs();
        }

        return -1;
    }

    public static PrimitiveEvent parse(String[] parts, String sourceNodeId) {
        // simple event: type | eventID | timestamp | eventType
        return new PrimitiveEvent(parts[3], parts[1], parseTimestamp(parts[2]), sourceNodeId);
    }
}
