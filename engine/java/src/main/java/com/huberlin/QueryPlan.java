package com.huberlin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class QueryPlan {
    @JsonProperty("nodes")
    public HashMap<String, QueryPlanNode> nodes;

    public void addFallbackInputs() {
        // Create an index about which complex input is supplied by which other query
        // Target node -> Query -> Source Queries
        HashMap<String, HashMap<String, List<QueryInformation.Processing>>> sources = new HashMap<>();
        for (HashMap.Entry<String, QueryPlanNode> node : nodes.entrySet()) {
            for (ForwardingRule rule : node.getValue().forward_rules) {
                String ruleEvent = rule.getEvent();
                if (ruleEvent.length() == 1) {
                    continue;
                }

                for (String to : rule.getTo()) {
                    if (!sources.containsKey(to)) {
                        sources.put(to, new HashMap<>());
                    }

                    HashMap<String, List<QueryInformation.Processing>> toMap = sources.get(to);
                    if (!toMap.containsKey(ruleEvent)) {
                        toMap.put(ruleEvent, new ArrayList<>());
                    }

                    QueryPlanNode sourceNode = nodes.get(rule.getFrom());
                    String sourceNodeId = rule.getFrom();
                    String prevNodeId = node.getKey();
                    while (!sourceNode.queries.containsKey(ruleEvent)) {
                        boolean found = false;
                        for (ForwardingRule sourceRule : sourceNode.forward_rules) {
                            if (sourceRule.getEvent().equals(ruleEvent) && sourceRule.getTo().contains(prevNodeId)) {
                                sourceNode = nodes.get(sourceRule.getFrom());
                                prevNodeId = sourceNodeId;
                                sourceNodeId = sourceRule.getFrom();
                                found = true;
                                break;
                            }
                        }

                        if (!found) {
                            throw new RuntimeException("Could not find source for projection " + ruleEvent);
                        }
                    }

                    toMap.get(rule.getEvent()).add(sourceNode.queries.get(ruleEvent));
                }
            }

            if (!sources.containsKey(node.getKey())) {
                sources.put(node.getKey(), new HashMap<>());
            }

            for (QueryInformation.Processing query : node.getValue().queries.values()) {
                HashMap<String, List<QueryInformation.Processing>> queryMap = sources.get(node.getKey());
                if (!queryMap.containsKey(query.query_name)) {
                    queryMap.put(query.query_name, new ArrayList<>());
                }

                queryMap.get(query.query_name).add(query);
            }
        }

        for (Map.Entry<String, QueryPlanNode> node : nodes.entrySet()) {
            for (QueryInformation.Processing query : node.getValue().queries.values()) {
                query.fallbackInputs = new HashMap<>();
                for (String input : query.inputs) {
                    if (input.startsWith("SEQ(") || input.startsWith("AND(")) {
                        HashSet<String> inputFallbacks = new HashSet<>();

                        for (QueryInformation.Processing sourceQuery : sources.get(node.getKey()).get(input)) {
                            inputFallbacks.addAll(sourceQuery.inputs);
                        }

                        query.fallbackInputs.put(input, inputFallbacks);
                    }
                }
            }
        }
    }

    public static QueryPlan fromFile(String filePath) throws IOException {
        final ObjectMapper mapper = new ObjectMapper();

        QueryPlan plan = mapper.readValue(new File(filePath), new TypeReference<QueryPlan>() { });
        plan.addFallbackInputs();

        return plan;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class QueryPlanNode {
        @JsonCreator
        public QueryPlanNode(
                @JsonProperty("forward_rules") Collection<ForwardingRule> forward_rules,
                @JsonProperty("queries") List<QueryInformation.Processing> queries,
                @JsonProperty("event_rates") HashMap<String, Integer> event_rates) {
            this.forward_rules = forward_rules;
            this.event_rates = event_rates;
            this.queries = new HashMap<>();
            for (QueryInformation.Processing query : queries) {
                this.queries.put(query.query_name, query);
            }
        }

        public Collection<ForwardingRule> forward_rules;
        public HashMap<String, QueryInformation.Processing> queries;

        public HashMap<String, Integer> event_rates;
    }
}
