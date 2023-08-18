package com.huberlin;

import java.util.Collection;

public class ForwardingRule {
    private String event;
    private String from;
    private Collection<String> to;

    public Collection<String> getTo() {
        return to;
    }

    public void setTo(Collection<String> to) {
        this.to = to;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }
}
