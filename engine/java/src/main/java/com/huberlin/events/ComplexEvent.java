package com.huberlin.events;

import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.lang.reflect.Type;
import java.time.Instant;
import java.util.*;

public class ComplexEvent extends Event {
    @TypeInfo(ChildrenMapTypeInfoFactory.class)
    private HashMap<String, PrimitiveEvent> children;
    private long highestTimestamp;
    private long lowestTimestamp;

    public ComplexEvent() {
        super();
    }

    public ComplexEvent(String eventType, String eventId, Instant timestamp, String sourceNodeId, PrimitiveEvent[] children) {
        super(eventType, eventId, timestamp, sourceNodeId);
        this.children = new HashMap<>(children.length);

        this.lowestTimestamp = Long.MAX_VALUE;
        for (PrimitiveEvent event : children) {
            long ts = event.getTimestampMs();

            if (ts < lowestTimestamp) {
                this.lowestTimestamp = ts;
            }

            if (ts > highestTimestamp) {
                this.highestTimestamp = ts;
            }

            this.children.put(event.getEventType(), event);
        }
    }

    @Override
    public long getHighestTimestamp() { return this.highestTimestamp; }

    public void setHighestTimestamp(long highestTimestamp) { this.highestTimestamp = highestTimestamp; }

    public void setLowestTimestamp(long lowestTimestamp) { this.lowestTimestamp = lowestTimestamp; }

    @Override
    public long getLowestTimestamp() { return this.lowestTimestamp; }

    public HashMap<String, PrimitiveEvent> getChildren() { return this.children; }

    public void setChildren(HashMap<String, PrimitiveEvent> children) { this.children = children; }

    @Override
    public Collection<PrimitiveEvent> deconstruct() {
        return this.children.values();
    }

    @Override
    public String toString() {
        StringBuilder eventString = new StringBuilder("complex");
        eventString.append(" | ").append(this.getEventId());
        eventString.append(" | ").append(this.getTimestamp());
        eventString.append(" | ").append(this.getEventType());
        eventString.append(" | ").append(this.children.size());
        eventString.append(" | ");

        int index = 0;
        for (PrimitiveEvent child : this.children.values()) {
            eventString.append("(").append(child.getTimestamp()).append(", ")
                    .append(child.getEventId()).append(", ").append(child.getEventType()).append(")");

            // no ";" after last event in the list
            if (index != this.children.size() - 1) {
                eventString.append(';');
            }

            index++;
        }

        return eventString.toString();
    }

    @Override
    public String getEventIdByEventType(String eventType) {
        PrimitiveEvent child = this.children.get(eventType);
        if (child != null) {
            return child.getEventId();
        }

        return null;
    }

    @Override
    public long getTimestampByEventType(String eventType) {
        PrimitiveEvent child = this.children.get(eventType);
        if (child != null) {
            return child.getTimestampMs();
        }

        return -1;
    }

    public static ComplexEvent parse(String[] parts, String sourceNodeId) {
        // complex event: type | eventID | timestamp [can be creationTime] | eventType | numberOfEvents | (individual Event);(individual Event)[;...]
        int numberOfEvents = Integer.parseInt(parts[4]);

        PrimitiveEvent[] children = new PrimitiveEvent[numberOfEvents];

        String[] childEventParts = parts[5].split(";");
        for(int i = 0; i < numberOfEvents; i++) {
            // one Event: (timestamp_hhmmssms , eventID , eventType)
            String[] eventParts = childEventParts[i].trim()
                    .substring(1, childEventParts[i].length() - 1)
                    .split(","); // entfernen von "(" und ")" + zerteilen

            children[i] = new PrimitiveEvent(eventParts[2], eventParts[1], parseTimestamp(eventParts[0]), sourceNodeId);
        }

        return new ComplexEvent(parts[3], parts[1], parseTimestamp(parts[2]), sourceNodeId, children);
    }

    public static class ChildrenMapTypeInfoFactory extends TypeInfoFactory<Map<String, PrimitiveEvent>> {
        @Override
        public TypeInformation<Map<String, PrimitiveEvent>> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
            return Types.MAP(Types.STRING, TypeInformation.of(PrimitiveEvent.class));
        }
    }
}
