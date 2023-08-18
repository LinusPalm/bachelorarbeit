package com.huberlin;

import com.huberlin.events.Event;
import org.apache.flink.cep.pattern.Pattern;

import java.util.*;

public class FallbackPattern {
    public static class FallbackQuery {
        private final Pattern<Event, ?> fallback;
        private final QueryInformation.Processing query;

        private final String combinedTrigger;

        public FallbackQuery(Pattern<Event, ?> fallback, QueryInformation.Processing query, String combinedTrigger) {
            this.fallback = fallback;
            this.query = query;
            this.combinedTrigger = combinedTrigger;
        }

        public Pattern<Event, ?> getFallback() { return this.fallback; }

        public QueryInformation.Processing getQuery() { return this.query; }

        public String getCombinedTrigger() { return combinedTrigger; }
    }

    private final Pattern<Event, ?> normal;
    private final HashMap<String, FallbackQuery> fallbacks;

    private final FallbackQuery combinedFallback;

    public FallbackPattern(
            Pattern<Event, ?> normal,
            HashMap<String, FallbackQuery> fallbacks,
            FallbackQuery combinedFallback) {
        this.normal = normal;
        this.fallbacks = fallbacks;
        this.combinedFallback = combinedFallback;
    }

    public Pattern<Event, ?> getNormal() {
        return normal;
    }

    public HashMap<String, FallbackQuery> getFallbacks() { return fallbacks; }

    public FallbackQuery getCombinedFallback() { return this.combinedFallback; }

    public HashSet<String> getPossibleFallbacks() { return new HashSet<>(fallbacks.keySet()); }

    public static FallbackPattern fromQuery(QueryInformation.Processing query) {
        Pattern<Event, ?> normal = PatternFactory.createWithControlPattern(query);

        if (query.inputs.size() > 2) {
            throw new RuntimeException("Queries with more than two inputs are not supported.");
        }

        String[] inputs = query.inputs.toArray(new String[0]);
        HashMap<String, FallbackQuery> fallbacks = new HashMap<>();

        boolean oneFallback = needsFallback(inputs[0]);
        boolean twoFallback = needsFallback(inputs[1]);

        if (oneFallback) {
            fallbacks.put(inputs[0], createFallbackForInput(query, inputs[0], twoFallback ? inputs[1] : null));
        }

        FallbackQuery combinedFallback = null;
        if (twoFallback) {
            fallbacks.put(inputs[1], createFallbackForInput(query, inputs[1], oneFallback ? inputs[0] : null));

            if (oneFallback) {
                combinedFallback = createCombinedFallback(query, inputs);
            }
        }

        return new FallbackPattern(normal, fallbacks, combinedFallback);
    }

    private static boolean needsFallback(String input) {
        return input.startsWith("SEQ(") || input.startsWith("AND(");
    }

    private static FallbackQuery createFallbackForInput(QueryInformation.Processing query, String input, String combinedTrigger) {
        QueryInformation.Processing fallbackQuery = new QueryInformation.Processing();
        fallbackQuery.inputs = new HashSet<>(query.inputs);
        fallbackQuery.inputs.remove(input);
        fallbackQuery.inputs.addAll(query.fallbackInputs.get(input));

        fallbackQuery.query_name = query.query_name;
        fallbackQuery.id_constraints = query.id_constraints;
        fallbackQuery.sequence_constraints = query.sequence_constraints;
        fallbackQuery.selectivity = query.selectivity;
        fallbackQuery.predicate_checks = query.predicate_checks;
        fallbackQuery.time_window_size = query.time_window_size;
        fallbackQuery.output_selection = query.output_selection;

        return new FallbackQuery(PatternFactory.createWithControlPattern(fallbackQuery), fallbackQuery, combinedTrigger);
    }

    private static FallbackQuery createCombinedFallback(QueryInformation.Processing query, String[] inputs) {
        QueryInformation.Processing fallbackQuery = new QueryInformation.Processing();
        fallbackQuery.inputs = new HashSet<>();
        fallbackQuery.inputs.addAll(query.fallbackInputs.get(inputs[0]));
        fallbackQuery.inputs.addAll(query.fallbackInputs.get(inputs[1]));

        fallbackQuery.query_name = query.query_name;
        fallbackQuery.id_constraints = query.id_constraints;
        fallbackQuery.sequence_constraints = query.sequence_constraints;
        fallbackQuery.selectivity = query.selectivity;
        fallbackQuery.predicate_checks = query.predicate_checks;
        fallbackQuery.time_window_size = query.time_window_size;
        fallbackQuery.output_selection = query.output_selection;

        return new FallbackQuery(PatternFactory.create(fallbackQuery), fallbackQuery, null);
    }
}
