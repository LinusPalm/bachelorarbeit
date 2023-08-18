package com.huberlin.events;

import java.util.Collection;

public class PartialMatchPrimitiveEvent extends Event {
    private PrimitiveEvent actualEvent;

    public PartialMatchPrimitiveEvent() {
        super();
    }

    public PartialMatchPrimitiveEvent(PrimitiveEvent event) {
        super(event.getEventType(), event.getEventId(), event.getTimestamp(), event.getSourceNodeId());
        actualEvent = event;
    }

    public PrimitiveEvent getActualEvent() { return this.actualEvent; }

    public void setActualEvent(PrimitiveEvent actualEvent) { this.actualEvent = actualEvent; }

    @Override
    public long getHighestTimestamp() {
        return actualEvent.getHighestTimestamp();
    }

    @Override
    public long getLowestTimestamp() {
        return actualEvent.getLowestTimestamp();
    }

    @Override
    public Collection<PrimitiveEvent> deconstruct() {
        return actualEvent.deconstruct();
    }

    @Override
    public String toString() {
        return actualEvent.toString();
    }

    @Override
    public String getEventIdByEventType(String eventType) {
        return actualEvent.getEventIdByEventType(eventType);
    }

    @Override
    public long getTimestampByEventType(String eventType) {
        return actualEvent.getTimestampByEventType(eventType);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object o) {
        return this == o || actualEvent.equals(o);
    }

    @Override
    public int hashCode() {
        return actualEvent.hashCode();
    }
}
