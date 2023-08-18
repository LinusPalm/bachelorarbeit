package com.huberlin.events;

import java.time.Instant;
import java.util.Collection;
import java.util.Objects;

public abstract class Event {
    private String eventType;
    private String eventId;
    private Instant timestamp;
    private String sourceNodeId;

    public Event() { }

    public Event(String eventType, String eventId, Instant timestamp, String sourceNodeId) {
        this.eventType = eventType.trim();
        this.eventId = eventId.trim();
        this.timestamp = timestamp;
        this.sourceNodeId = sourceNodeId;
    }

    public String getEventType() { return this.eventType; }

    public String getEventId() { return this.eventId; }

    public long getTimestampMs() { return this.timestamp.toEpochMilli(); }

    public long getTimestampNs() { return this.timestamp.getEpochSecond() * 1000 * 1_000_000 + this.timestamp.getNano(); }

    public Instant getTimestamp() { return this.timestamp; }

    public String getSourceNodeId() { return this.sourceNodeId; }

    public void setEventType(String eventType) { this.eventType = eventType; }

    public void setEventId(String eventId) { this.eventId = eventId; }

    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }

    public void setSourceNodeId(String sourceNodeId) { this.sourceNodeId = sourceNodeId; }

    public abstract long getHighestTimestamp();

    public abstract long getLowestTimestamp();

    public abstract Collection<PrimitiveEvent> deconstruct();

    public abstract String toString();

    public abstract String getEventIdByEventType(String eventType);

    public abstract long getTimestampByEventType(String eventType);

    public boolean isControl() { return false; }

    public boolean isControl(String controlType) { return false; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return Objects.equals(eventId, event.eventId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId);
    }

    public static Event parse(String rawEvent, String sourceNodeId) {
        String[] parts = rawEvent.split(" \\| ");
        switch (parts[0]) {
            case "simple":
                return PrimitiveEvent.parse(parts, sourceNodeId);
            case "complex":
                return ComplexEvent.parse(parts, sourceNodeId);
            case "control":
                return ControlEvent.parse(parts, sourceNodeId);
            default:
                throw new RuntimeException("Invalid event type: " + parts[0]);
        }
    }

    public static Instant parseTimestamp(String value) {
        return Instant.parse(value);
    }
}
