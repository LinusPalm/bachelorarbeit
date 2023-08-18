package com.huberlin;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.huberlin.events.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class ForwardRuleProcessor {
    public static final String REPLACE_MODE_PROJECTION = "projection";
    public static final String REPLACE_MODE_SOURCE = "source";

    private transient final Logger logger = LoggerFactory.getLogger(ForwardRuleProcessor.class);

    private final Map<String, HashSet<String>> forwardRules;
    private final Collection<String> allDestinations;
    private final String ownNodeId;
    private final HashMap<String, Collection<String>> eventSources;

    public ForwardRuleProcessor(Map<String, HashSet<String>> forwardRules, Collection<String> allDestinations, String ownNodeId, HashMap<String, Collection<String>> eventSources) {
        this.forwardRules = forwardRules;
        this.allDestinations = allDestinations;
        this.ownNodeId = ownNodeId;
        this.eventSources = eventSources;
    }

    public Map<String, HashSet<String>> getForwardRules() { return forwardRules; }

    public Collection<String> getAllDestinations() { return this.allDestinations; }

    public boolean shouldForward(Event event) {
        if (event.isControl()) {
            // Never forward control messages
            return false;
        }

        String forwardKey = event.getEventType() + ":" + event.getSourceNodeId();
        return forwardRules.containsKey(forwardKey);
    }

    public Collection<String> getForwardDestinations(Event event) {
        String forwardKey = event.getEventType() + ":" + event.getSourceNodeId();
        return forwardRules.get(forwardKey);
    }

    private void mergeDestinations(Collection<String> oldDestinations, HashSet<String> destinations, String projection, String fromNodeId) {
        for (String oldDest : oldDestinations) {
            if (destinations.add(oldDest)) {
                System.out.println("[ForwardRule] New rule: " + projection + " from " + fromNodeId + " to " + oldDest);
            } else {
                System.out.println("[ForwardRule] New rule exists already: " + projection + " from " + fromNodeId + " to " + oldDest);
            }
        }
    }

    private void replaceRuleProjections(String replacement, String source, Collection<String> oldDestinations) {
        String key = replacement + ":" + source;
        HashSet<String> destinations;
        if (forwardRules.containsKey(key)) {
            destinations = forwardRules.get(key);
        } else {
            destinations = new HashSet<>();
            forwardRules.put(key, destinations);
        }

        mergeDestinations(oldDestinations, destinations, replacement, source);
    }

    public void replaceForwardRules(String pattern, String fromNodeId, String replaceMode, Collection<String> replacements, boolean removeOld) {
        logger.warn("Replacing forward rules for " + pattern + " from " + fromNodeId + " with " + replacements + "; Mode: " + replaceMode);
        String forwardKey = pattern + ":" + fromNodeId;
        Collection<String> oldDestinations = forwardRules.get(forwardKey);
        if (oldDestinations != null) {
            if (replaceMode.equalsIgnoreCase(REPLACE_MODE_PROJECTION)) {
                for (String replacement : replacements) {
                    Collection<String> sources = getSourcesFor(this.eventSources, replacement);

                    // Replace rules for harmful projection with rules for input
                    sources.add(fromNodeId);

                    for (String source : sources) {
                        replaceRuleProjections(replacement, source, oldDestinations);
                    }

                    //replaceRuleProjections(replacement, fromNodeId, oldDestinations);
                }

                if (removeOld) {
                    forwardRules.remove(forwardKey);
                }
            }
            else if (replaceMode.equalsIgnoreCase(REPLACE_MODE_SOURCE)) {
                String newSource = Collections.enumeration(replacements).nextElement();
                String replacementKey = pattern + ":" + newSource;
                HashSet<String> existingDestinations = forwardRules.computeIfAbsent(replacementKey, k -> new HashSet<>());

                mergeDestinations(oldDestinations, existingDestinations, pattern, newSource);

                if (removeOld) {
                    forwardRules.remove(forwardKey);
                }
            }
            else {
                logger.error("Unknown change_rules mode: " + replaceMode);
            }
        }
        else {
            System.out.print("[ForwardRule] No new rules");
        }
    }

    private static Collection<String> getSourcesFor(HashMap<String, Collection<String>> eventSources, String eventType) {
        Collection<String> sources;
        if (eventSources.containsKey(eventType)) {
            sources = eventSources.get(eventType);
        }
        else {
            sources = new HashSet<>();
            eventSources.put(eventType, sources);
        }

        return sources;
    }

    public static ForwardRuleProcessor fromPlan(QueryPlan plan, String ownNodeId) {
        HashMap<String, HashSet<String>> ownRules = null;
        HashSet<String> allDestinations = new HashSet<>();
        HashMap<String, Collection<String>> eventSources = new HashMap<>();

        for (Map.Entry<String, QueryPlan.QueryPlanNode> node : plan.nodes.entrySet()) {
            if (node.getKey().equals(ownNodeId)) {
                ownRules = new HashMap<>();
                for (ForwardingRule rule : node.getValue().forward_rules) {
                    // There can be different forward rules for the same event type depending on the event source
                    String eventKey = rule.getEvent() + ":" + rule.getFrom();
                    ownRules.put(eventKey, new HashSet<>(rule.getTo()));
                    allDestinations.addAll(rule.getTo());
                }

                for (String queryName : node.getValue().queries.keySet()) {
                    getSourcesFor(eventSources, queryName).add(node.getKey());
                }

                for (Map.Entry<String, Integer> eventType : node.getValue().event_rates.entrySet()) {
                    if (eventType.getValue() > 0) {
                        getSourcesFor(eventSources, eventType.getKey()).add(node.getKey());
                    }
                }
            }
            else {
                for (ForwardingRule rule : node.getValue().forward_rules) {
                    if (rule.getTo().contains(ownNodeId)) {
                        getSourcesFor(eventSources, rule.getEvent()).add(node.getKey());
                    }
                }
            }
        }

        if (ownRules == null) {
            throw new RuntimeException("Forward rules for node " + ownNodeId + " not found in plan.");
        }

        return new ForwardRuleProcessor(ownRules, allDestinations, ownNodeId, eventSources);
    }
}
