package xuwei.tech.watermark;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;


import javax.annotation.Nullable;

public class MyWatermark implements AssignerWithPeriodicWatermarks<Tuple3<Long,String,String>> {
    Long currentMaxTimestamp = 0L;
    final Long maxOutOrderness =  10000L; //最大允许乱序时间10s

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp - maxOutOrderness);
    }

    @Override
    public long extractTimestamp(Tuple3<Long, String, String> element, long previousElementTimestamp) {
        Long timestamp = element.f0;
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }
}
