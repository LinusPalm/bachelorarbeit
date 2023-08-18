package com.huberlin;

import com.huberlin.events.ControlEvent;
import com.huberlin.events.Event;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Objects;

public class Patterns {
    public static IterativeCondition<Event> createControlPattern(final String controlType, final String predicate) {
        if (controlType == null) {
            throw new NullPointerException("Control type must not be null.");
        }

        if (predicate == null) {
            throw new NullPointerException("Control pattern predicate must not be null.");
        }

        return new SimpleCondition<Event>() {
            @Override
            public boolean filter(final Event value) {
                return value.isControl(controlType) && ((ControlEvent)value).getParameter().equals(predicate);
            }
        };
    }

    /*public static FallbackPattern createPattern1() {
        // SEQ(SEQ(A,B),C)
        // Left fallback: SEQ(A,B,C)
        // Right fallback: None
        return new FallbackPattern(
                Pattern.<Event>begin("control")
                        .where(createControlPattern("switch_pattern", "SEQ(A,B)"))
                        .or(createControlPattern("forward_inputs", "SEQ(A,B,C)")).optional()
                        .followedBy(Pattern.<Event>begin("first").where(eventTypeEquals("SEQ(A,B)"))
                                .followedByAny("second").where(eventTypeEquals("C")))
                        .optional(),
                "SEQ(A,B)",
                Pattern.<Event>begin("first").where(eventTypeEquals("A"))
                        .followedByAny("second").where(eventTypeEquals("B"))
                        .followedByAny("third").where(eventTypeEquals("C"))
                        .within(Time.minutes(3)),
                "C",
                null
        );
    }

    public static FallbackPattern createPatternForNode0() {
        // AND(A,B) from A,B
        return new FallbackPattern(
                Pattern.<Event>begin("first").where(new IterativeCondition<Event>() {
                    @Override
                    public boolean filter(Event event, Context<Event> context) throws Exception {
                        if (event.getEventType().equals("A") || event.getEventType().equals("B")) {
                            for (Event before : context.getEventsForPattern("first")) {
                                if (before.getEventType().equals(event.getEventType())) {
                                    return false;
                                }
                            }

                            return true;
                        }

                        return false;
                    }
                }).times(2).allowCombinations().within(Time.days(1)),
                "A",
                null,
                "B",
                null
        );
    }

    public static FallbackPattern createPatternForNode1() {
        // AND(C,A,B) from AND(A,B), C
        // with fallback AND(C,A,B) from A,B,C
        return new FallbackPattern(
                Pattern.<Event>begin("first").where(new IterativeCondition<Event>() {
                    @Override
                    public boolean filter(Event event, Context<Event> context) throws Exception {
                        if (event.getEventType().equals("AND(A,B)") || event.getEventType().equals("C")) {
                            for (Event before : context.getEventsForPattern("first")) {
                                if (before.getEventType().equals(event.getEventType())) {
                                    return false;
                                }
                            }

                            return true;
                        }

                        return false;
                    }
                })
                .times(2)
                .allowCombinations()
                .or(createControlPattern("switch_pattern", "AND(A,B)"))
                .or(createControlPattern("forward_inputs", "AND(C,A,B)"))
                .within(Time.days(1)),
                "AND(A,B)",
                Pattern.<Event>begin("first").where(new IterativeCondition<Event>() {
                    @Override
                    public boolean filter(Event event, Context<Event> context) throws Exception {
                        if (event.getEventType().equals("A") || event.getEventType().equals("B") || event.getEventType().equals("C")) {
                            for (Event before : context.getEventsForPattern("first")) {
                                if (before.getEventType().equals(event.getEventType())) {
                                    return false;
                                }
                            }

                            return true;
                        }

                        return false;
                    }
                }).times(3).allowCombinations().within(Time.days(1)), "C", null);
    }*/

    public static SimpleCondition<Event> eventTypeEquals(String type) {
        return new SimpleCondition<Event>()
        {
            @Override
            public boolean filter(Event value) {
                return value.getEventType().equals(type);
            }
        };
    }
}
