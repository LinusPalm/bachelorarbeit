package com.huberlin.events;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;

public class ControlEvent extends Event {
    private String[] parameters;

    public ControlEvent() {
        super();
    }

    public ControlEvent(String controlType, String[] parameters, Instant timestamp, String sourceNodeId) {
        super(controlType, "", timestamp, sourceNodeId);
        this.parameters = parameters;
    }

    public String[] getParameters() { return this.parameters; }

    public void setParameters(String[] parameters) { this.parameters = parameters; }

    public String getParameter(int index) {
        if (index < this.parameters.length) {
            return this.parameters[index];
        }

        return null;
    }

    public String getParameter() {
        return this.parameters[0];
    }

    @Override
    public long getHighestTimestamp() { throw new UnsupportedOperationException("Control events do not have timestamps."); }

    @Override
    public long getLowestTimestamp() { throw new UnsupportedOperationException("Control events do not have timestamps."); }

    @Override
    public String getEventIdByEventType(String eventType) { return null; }

    @Override
    public long getTimestampByEventType(String eventType) { throw new UnsupportedOperationException("Control events do not have timestamps."); }

    @Override
    public Collection<PrimitiveEvent> deconstruct() { throw new UnsupportedOperationException("Control events can't be deconstructed."); }

    @Override
    public String toString() {
        return "control | " + getEventType() + " | " + String.join(" | ", this.parameters);
    }

    @Override
    public boolean isControl() { return true; }

    @Override
    public boolean isControl(String controlType) { return this.getEventType().equals(controlType); }

    public static ControlEvent parse(String[] parts, String sourceNodeId) {
        // control | controlType | parameter1 | parameter2 | ... | parameterN
        return new ControlEvent(parts[1], Arrays.copyOfRange(parts, 2, parts.length), Instant.now(), sourceNodeId);
    }
}
