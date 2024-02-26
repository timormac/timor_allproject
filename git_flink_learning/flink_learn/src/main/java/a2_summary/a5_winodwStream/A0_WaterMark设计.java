package a2_summary.a5_winodwStream;

import a1_flink_learn.dao.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @Author Timor
 * @Date 2024/2/26 9:53
 * @Version 1.0
 */
public class A0_WaterMark设计 {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.fromElements("a", "b");

        //TODO 可调用如下方法
        WatermarkStrategy<String> strategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))//水位线延迟2s更新
                .withIdleness(Duration.ofSeconds(6)) //空闲等待时间6s,到6s自动更新水位线
                .withTimestampAssigner((s, time) -> 1000L) //指定事件时间
                ;

        //TODO 流指定水位线规则
        ds.assignTimestampsAndWatermarks(strategy);


        //TODO 自定义水位线规则，周期间隔100ms更新/每条数据更新
        /**
         * WatermarkStrategy有4种水位线更新规则
         * .noWatermarks()
         * .forMonotonousTimestamps(null)
         * .forGenerator(null)
         * forBoundedOutOfOrderness(null)
         */
        WatermarkStrategy<Object> st = WatermarkStrategy.forGenerator(
                new WatermarkGeneratorSupplier(){
                    @Override
                    public WatermarkGenerator createWatermarkGenerator(Context context) {
                        return new PeriodGenerator(1000);
                    }
                }
        );




    }
}

class PeriodGenerator<T> implements WatermarkGenerator<T> {

    // 乱序等待时间
    private long delayTs;
    // 用来保存 当前为止 最大的事件时间
    private long maxTs;

    public PeriodGenerator(long delayTs) {
        this.delayTs = delayTs;
        this.maxTs = Long.MIN_VALUE + this.delayTs + 1;
    }

    /**
     * 每条数据来，都会调用一次： 用来提取最大的事件时间，保存下来
     */
    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        maxTs = Math.max(maxTs, eventTimestamp);
        System.out.println("调用onEvent方法，获取目前为止的最大时间戳=" + maxTs);
    }

    /**
     * 周期性调用： 发射 watermark
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(maxTs - delayTs - 1));
        System.out.println("调用onPeriodicEmit方法，生成watermark=" + (maxTs - delayTs - 1));
    }
}


class MyGenerator<T> implements WatermarkGenerator<T> {

    // 乱序等待时间
    private long delayTs;
    // 用来保存 当前为止 最大的事件时间
    private long maxTs;

    public MyGenerator(long delayTs) {
        this.delayTs = delayTs;
        this.maxTs = Long.MIN_VALUE + this.delayTs + 1;
    }

    /**
     * 每条数据来，都会调用一次： 用来提取最大的事件时间，保存下来,并发射watermark
     */
    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        maxTs = Math.max(maxTs, eventTimestamp);
        output.emitWatermark(new Watermark(maxTs - delayTs - 1));
        System.out.println("调用onEvent方法，获取目前为止的最大时间戳=" + maxTs+",watermark="+(maxTs - delayTs - 1));
    }

    /**
     * 周期性调用： 不需要
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {

    }
}
