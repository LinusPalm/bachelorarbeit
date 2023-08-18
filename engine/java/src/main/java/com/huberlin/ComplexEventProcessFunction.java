package com.huberlin;

import com.huberlin.events.*;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.nfa.ComputationState;
import org.apache.flink.cep.nfa.NFAState;
import org.apache.flink.cep.nfa.NFAStateSerializer;
import org.apache.flink.cep.nfa.sharedbuffer.EventId;
import org.apache.flink.cep.nfa.sharedbuffer.NodeId;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBuffer;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferAccessor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.*;

public class ComplexEventProcessFunction extends PatternProcessFunction<Event, Event> implements TimedOutPartialMatchHandler<Event> {
    private final Logger logger = LoggerFactory.getLogger(ComplexEventProcessFunction.class);
    private final String inputs;
    private SharedBuffer<Event> sharedBuffer;
    private final String patternName;
    private final String nodeId;
    private final OutputTag<Event> partialMatchTag;
    private final OutputTag<Event> forwardPartialMatchTag;

    private final boolean fallbackEnabled;

    private transient boolean hasReinserted = false;

    public ComplexEventProcessFunction(String patternName,
                                       String nodeId,
                                       String inputs,
                                       @Nullable OutputTag<Event> partialMatchTag,
                                       @Nullable OutputTag<Event> forwardPartialMatchTag) {
        super();

        this.patternName = patternName;
        this.nodeId = nodeId;
        this.inputs = inputs;
        this.partialMatchTag = partialMatchTag;
        this.forwardPartialMatchTag = forwardPartialMatchTag;
        this.fallbackEnabled = partialMatchTag != null && this.forwardPartialMatchTag != null;
    }

    @Override
    public void setRuntimeContext(RuntimeContext t) {
        // CepOperator sets a limited version of the RuntimeContext (CepRuntimeContext) that can't access state.
        // HACK: Just block the override of the original RuntimeContext here. Everything still seems to work fine...
        if (t instanceof StreamingRuntimeContext) {
            super.setRuntimeContext(t);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // The shared buffer manages access to the actual events associated with partial pattern matches.
        // We can't access the CepOperator's internal buffer, but we can create our own, that will access
        // the same state as the internal SharedBuffer.
        // The SharedBuffer requires a KeyedStateStore instance, which we also can't access.
        // Luckily RuntimeContext has access to it and methods that wrap the KeyedStateStore methods that we need.
        // So we create a DummyKeyedStateStore which just calls the corresponding methods on the RuntimeContext.
        sharedBuffer = new SharedBuffer<>(
                new DummyKeyedStateStore(getRuntimeContext()),
                TypeInformation.of(Event.class).createSerializer(getRuntimeContext().getExecutionConfig()));
    }

    @Override
    public void processMatch(Map<String, List<Event>> match, Context ctx, Collector<Event> out) throws Exception {
        if (!match.containsKey("control")) {
            HashSet<PrimitiveEvent> events = new HashSet<>();

            boolean allReinserted = true;
            //Instant maxTimestamp = Instant.MIN;
            for (Map.Entry<String, List<Event>> entry : match.entrySet()) {
                for (Event event : entry.getValue()) {
                    events.addAll(event.deconstruct());

                    if (!(event instanceof PartialMatchPrimitiveEvent) && !(event instanceof PartialMatchComplexEvent)) {
                        allReinserted = false;
                    }
                }
            }

            // To prevent duplicate matches after reinserting partial matches from another pattern
            // only match events that contain at least one new event
            if (!allReinserted) {
                // Create event id from hash of pattern name and sorted primitive child event ids
                String[] ids = new String[events.size() + 1];
                ids[0] = patternName;
                int i = 1;
                for (Event e : events) {
                    ids[i++] = e.getEventId();
                }

                Arrays.sort(ids, 1, ids.length);
                String complexEventId = Integer.toHexString(Arrays.hashCode(ids));

                Event complexEvent = new ComplexEvent(
                        patternName,
                        complexEventId,
                        Instant.now(),
                        nodeId,
                        events.toArray(new PrimitiveEvent[0]));

                // Complex events are added back to the local event stream
                out.collect(complexEvent);
            }
            else {
                System.out.println("[Control] Duplicate match discarded.");
            }
        }
        else if (fallbackEnabled) {
            // Event is control message -> Output partial matches of this pattern to the side output
            RuntimeContext runtimeContext = getRuntimeContext();
            ValueState<NFAState> nfaState = runtimeContext
                    .getState(new ValueStateDescriptor<>("nfaStateName", new NFAStateSerializer()));

            HashSet<String> partialMatchOutput = new HashSet<>();

            // Get partial matches as states of the internal pattern NFA
            NFAState nfaStateValue = nfaState.value();
            if (nfaStateValue != null) {
                Queue<ComputationState> partialMatches = nfaState.value().getPartialMatches();

                // When forward_inputs occurs, new forward rules are added for the inputs of a pattern,
                // which might be complex events as well.
                // If complex partial matches would be deconstructed, they would not be correctly forwarded by those
                // new forwarding rules.
                ControlEvent controlEvent = (ControlEvent)match.get("control").get(0);
                boolean shouldDeconstruct = !controlEvent.isControl("forward_inputs");

                // Turn the partial matches back into the corresponding events that triggered them
                try (SharedBufferAccessor<Event> accessor = sharedBuffer.getAccessor()) {
                    for (ComputationState s : partialMatches) {
                        // Only output states that actually started
                        if (s.getStartEventID() != null) {
                            // Being in a state means, that the event of that state hasn't been matched yet,
                            // so to get the partial match, we only need the previous states
                            NodeId previousEntry = s.getPreviousBufferEntry();

                            // Get the internal flink event id for each state from the start state to the current partial match state(s)
                            List<Map<String, List<EventId>>> extractedPatterns = accessor.extractPatterns(previousEntry, s.getVersion());
                            if (extractedPatterns.size() == 1) {
                                if (extractedPatterns.get(0).containsKey("control")) {
                                    // Previous control events are unfortunately included in partial matches
                                    continue;
                                }

                                // Turn internal flink event ids back into events
                                // Map: State name (from pattern definition) -> events associated with that state
                                Map<String, List<Event>> partialMatchEvents = accessor.materializeMatch(extractedPatterns.get(0));
                                for (List<Event> entry : partialMatchEvents.values()) {
                                    for (Event event : entry) {
                                        if (shouldDeconstruct) {
                                            List<Event> toBeReinserted = new ArrayList<>();
                                            if (event.getEventType().equals(controlEvent.getParameter()) || hasReinserted) {
                                                toBeReinserted.addAll(event.deconstruct());
                                            }
                                            else {
                                                toBeReinserted.add(event);
                                            }

                                            // An event can be part of multiple partial matches,
                                            // but we only want each unique event once.
                                            for (Event complexEventPart : toBeReinserted) {
                                                if (!partialMatchOutput.contains(complexEventPart.getEventId())) {
                                                    Event partialMatchEvent;
                                                    if (complexEventPart instanceof ComplexEvent) {
                                                        partialMatchEvent = new PartialMatchComplexEvent((ComplexEvent) complexEventPart);
                                                    }
                                                    else if (complexEventPart instanceof PrimitiveEvent) {
                                                        partialMatchEvent = new PartialMatchPrimitiveEvent((PrimitiveEvent) complexEventPart);
                                                    }
                                                    else {
                                                        continue;
                                                    }

                                                    ctx.output(this.partialMatchTag, partialMatchEvent);
                                                    partialMatchOutput.add(complexEventPart.getEventId());

                                                    System.out.println("[Control] Partial match \"" + complexEventPart + "\" reinserted from " + inputs);
                                                }
                                            }
                                        }
                                        else {
                                            if (!partialMatchOutput.contains(event.getEventId())) {
                                                // Output partial match, so it can be forwarded
                                                ctx.output(forwardPartialMatchTag, event);
                                                partialMatchOutput.add(event.getEventId());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                System.out.println("[Control] Reinserted " + partialMatchOutput.size() + " partial matches");
                hasReinserted = true;
            }
        }
    }

    @Override
    public void processTimedOutMatch(Map<String, List<Event>> match, Context ctx) throws Exception {
        logger.warn("Match timed out: " + match + ", Timestamp: " + ctx.timestamp());
    }
}
