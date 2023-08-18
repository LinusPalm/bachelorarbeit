package com.huberlin;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.*;

public class DummyKeyedStateStore implements KeyedStateStore {
    private final RuntimeContext runtimeContext;

    public DummyKeyedStateStore(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

    @Override
    public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
        return runtimeContext.getState(stateProperties);
    }

    @Override
    public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
        return runtimeContext.getListState(stateProperties);
    }

    @Override
    public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
        return runtimeContext.getReducingState(stateProperties);
    }

    @Override
    public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
        return runtimeContext.getAggregatingState(stateProperties);
    }

    @Override
    public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
        return runtimeContext.getMapState(stateProperties);
    }
}
