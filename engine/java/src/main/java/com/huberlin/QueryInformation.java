package com.huberlin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.huberlin.events.Event;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class QueryInformation implements Serializable {
    public Processing processing;

    public static QueryInformation fromFile(String filePath) {
        ObjectMapper mapper = new ObjectMapper();
        QueryInformation info;

        try {
            info = mapper.readValue(new File(filePath), new TypeReference<QueryInformation>() {});
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return info;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Processing implements Serializable {
        public Processing() {}

        @JsonCreator
        public Processing(
                @JsonProperty("query_name") String query_name,
                @JsonProperty("output_selection") List<String> output_selection,
                @JsonProperty("inputs") HashSet<String> inputs,
                @JsonProperty("selectivity") double selectivity,
                @JsonProperty(value = "selectivity2", required = false) Double selectivity2,
                @JsonProperty(value = "selectivity2_after", required = false) Long selectivity2_after,
                @JsonProperty("sequence_constraints") List<List<String>> sequence_constraints,
                @JsonProperty("id_constraints") List<String> id_constraints,
                @JsonProperty("time_window_size") long time_window_size,
                @JsonProperty("predicate_checks") long predicate_checks) {
            this.query_name = query_name;
            this.output_selection = output_selection;
            this.inputs = inputs;
            this.selectivity = selectivity;
            this.selectivity2 = selectivity2;
            this.selectivity2_after = selectivity2_after;
            this.sequence_constraints = new ArrayList<>(sequence_constraints.size());
            for (List<String> constraint : sequence_constraints) {
                this.sequence_constraints.add(new SequenceConstraint(constraint.get(0), constraint.get(1)));
            }

            this.id_constraints = id_constraints;
            this.time_window_size = time_window_size;
            this.predicate_checks = predicate_checks;
        }

        public String query_name;
        public List<String> output_selection;
        public HashSet<String> inputs;
        public double selectivity;
        public Double selectivity2;
        public Long selectivity2_after;
        public List<SequenceConstraint> sequence_constraints;
        public List<String> id_constraints;
        public long time_window_size;
        public long predicate_checks;

        public HashMap<String, Collection<String>> fallbackInputs;

        @Override
        public String toString() {
            return query_name + " FROM " + inputs;
        }
    }

    public static class SequenceConstraint implements Serializable {
        private final String before;
        private final String after;

        public SequenceConstraint(String before, String after) {
            this.before = before;
            this.after = after;
        }

        public boolean isViolatedBy(Event a, Event b) {
            long timestampBefore = a.getTimestampByEventType(before);
            long timestampAfter = b.getTimestampByEventType(after);

            return timestampBefore != -1 && timestampAfter != -1 && timestampAfter <= timestampBefore;
        }
    }
}
