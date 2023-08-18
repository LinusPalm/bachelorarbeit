package com.huberlin;

import com.huberlin.events.ComplexEvent;
import com.huberlin.events.Event;
import org.apache.flink.api.common.eventtime.*;

import java.time.Instant;

public class CustomWatermarkStrategy implements WatermarkStrategy<Event> {
    @Override
    public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new CustomWatermarkGenerator();
    }

    @Override
    public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new CustomTimestampAssigner();
    }

    public static class CustomTimestampAssigner implements TimestampAssigner<Event> {
        private static final long ONE_UNIT = 1_000_000;
        private long prevTimestamp = 0;

        @Override
        public long extractTimestamp(Event element, long recordTimestamp) {
            Instant now = Instant.now();
            long timestamp = now.getEpochSecond() * 1000 * 1_000_000 + now.getNano();
            if (timestamp == prevTimestamp) {
                timestamp += ONE_UNIT;
            }

            prevTimestamp = timestamp;
            return timestamp;
        }
    }

    public static class CustomWatermarkGenerator implements WatermarkGenerator<Event> {
        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            output.emitWatermark(new Watermark(eventTimestamp));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) { }
    }
}
