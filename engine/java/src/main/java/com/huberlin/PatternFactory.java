package com.huberlin;

import com.huberlin.events.ControlEvent;
import com.huberlin.events.Event;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class PatternFactory {
    private static final long WITHIN_TIME = Long.MAX_VALUE;
    private static final Logger logger = LoggerFactory.getLogger(PatternFactory.class);

    public static SimpleCondition<Event> createControlCondition(String queryName, Set<String> queryInputs) {
        return new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event value) {
                if (value.isControl()) {
                    ControlEvent controlEvent = (ControlEvent) value;
                    if (controlEvent.isControl("forward_inputs")) {
                        return controlEvent.getParameter().equals(queryName);
                    }
                    else if (controlEvent.isControl("switch_pattern")) {
                        return queryInputs.contains(controlEvent.getParameter());
                    }
                }

                return false;
            }
        };
    }

    public static Pattern<Event, ?> createWithControlPattern(QueryInformation.Processing queryInfo) {
        return Pattern.<Event>begin("control")
                .where(createControlCondition(queryInfo.query_name, new HashSet<>(queryInfo.inputs)))
                .optional()
                .followedBy("first")
                .where(createCondition(queryInfo))
                .times(queryInfo.inputs.size())
                .optional()
                .allowCombinations()
                .within(Time.of(WITHIN_TIME, TimeUnit.NANOSECONDS));
    }

    public static Pattern<Event, Event> create(QueryInformation.Processing query_information) {
        return Pattern.<Event>begin("first")
                .where(createCondition(query_information))
                .times(query_information.inputs.size())
                .allowCombinations()
                .within(Time.of(WITHIN_TIME, TimeUnit.NANOSECONDS));
    }

    private static IterativeCondition<Event> createCondition(QueryInformation.Processing queryInfo) {
        final long TIME_WINDOW_SIZE_MS = queryInfo.time_window_size * 1_000;
        final boolean hasSelectivitySwitch = queryInfo.selectivity2 != null && queryInfo.selectivity2_after != null;
        if (hasSelectivitySwitch) {
            Instant now = Instant.now().plusSeconds(queryInfo.selectivity2_after);
            queryInfo.selectivity2_after = now.toEpochMilli();
        }

        return new IterativeCondition<Event>() {
            final Random rand = new Random();
            private boolean usedSelectivity2 = false;

            @Override
            public boolean filter(Event new_event, Context<Event> ctx) throws Exception {
                if (!queryInfo.inputs.contains(new_event.getEventType())) {
                    return false;
                }

                List<Event> prevEvents = new ArrayList<>(queryInfo.inputs.size());
                long highestTimestamp = new_event.getHighestTimestamp();
                long lowestTimestamp = new_event.getLowestTimestamp();

                for (Event e : ctx.getEventsForPattern("first")) {
                    if (e.getEventType().equals(new_event.getEventType())) {
                        return false;
                    }

                    if (e.getHighestTimestamp() > highestTimestamp) {
                        highestTimestamp = e.getHighestTimestamp();
                    }

                    if (e.getLowestTimestamp() < lowestTimestamp) {
                        lowestTimestamp = e.getLowestTimestamp();
                    }

                    prevEvents.add(e);
                }

                // First event, no other checks required
                if (prevEvents.size() == 0) {
                    return true;
                }

                // Check if the difference between the highest and lowest timestamps violates the time window constraint
                if (Math.abs(highestTimestamp - lowestTimestamp) > TIME_WINDOW_SIZE_MS) {
                    return false;
                }

                for (String id_constraint : queryInfo.id_constraints) {
                    String newEventId = new_event.getEventIdByEventType(id_constraint);

                    if (newEventId != null) {
                        for (Event previous : prevEvents) {
                            String prevEventId = previous.getEventIdByEventType(id_constraint);
                            if (prevEventId != null && !prevEventId.equals(newEventId)) {
                                return false;
                            }
                        }
                    }
                }

                for (QueryInformation.SequenceConstraint sequenceConstraint : queryInfo.sequence_constraints) {
                    for (Event previous : prevEvents) {
                        if (sequenceConstraint.isViolatedBy(previous, new_event) ||
                                sequenceConstraint.isViolatedBy(new_event, previous)) {
                            return false;
                        }
                    }
                }

                // Only check selectivity once at the end
                if (prevEvents.size() == queryInfo.inputs.size() - 1) {
                    double selectivity;
                    if (hasSelectivitySwitch && highestTimestamp > queryInfo.selectivity2_after) {
                        selectivity = queryInfo.selectivity2;
                        if (!usedSelectivity2) {
                            logger.warn("Using SECOND selectivity for " + queryInfo.query_name);
                            usedSelectivity2 = true;
                        }
                    }
                    else {
                        selectivity = queryInfo.selectivity;
                    }

                    for (int i = 0; i < queryInfo.predicate_checks; i++) {
                        if (rand.nextDouble() > Math.pow(selectivity, (1.0 / queryInfo.predicate_checks))) {
                            if (logger.isDebugEnabled()) {
                                logger.debug("[Selectivity] Discarded match for " + queryInfo.query_name);
                            }
                            return false;
                        }
                    }
                }

                return true;
            }
        };
    }
}
