package a2_summary.a3_winodwStream;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import a2_summary.a1_envAndStream.A4_POJO;

/**
 * @Title: A2_KeyedWindow
 * @Package: summary.a3_winodwStream
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/24 19:17
 * @Version:1.0
 */
public class A2_KeyedWindow {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.fromElements("a", "b");
        KeyedStream<String, String> keyed = ds.keyBy(s -> "a");

        /**
         * DataStream只提供了WindowAll
         * keyby的流有window和windowAll
         * connect流没有窗口
         */

        //滚动 处理时间
        TumblingProcessingTimeWindows tmProcess = TumblingProcessingTimeWindows.of( Time.seconds(1000L) );
        //滚动 事件时间
        TumblingEventTimeWindows tmEvent = TumblingEventTimeWindows.of(Time.seconds(1000L));
        //滑动 处理时间
        SlidingProcessingTimeWindows slidPro = SlidingProcessingTimeWindows.of(Time.seconds(100), Time.seconds(10));
        //滑动 事件时间
        SlidingEventTimeWindows slidEv = SlidingEventTimeWindows.of(Time.seconds(100), Time.seconds(10));
        //会话 事件时间
        EventTimeSessionWindows sessionEv = EventTimeSessionWindows.withGap(Time.seconds(100));
        //会话 处理时间
        ProcessingTimeSessionWindows sessionPro = ProcessingTimeSessionWindows.withGap(Time.seconds(100));
        //全局窗口,需要自己定义触发器，不然没意义
        GlobalWindows globalWindows = GlobalWindows.create();

        //相比windowAll多了一个范性
        WindowedStream<String, String, TimeWindow> window = keyed.window(tmProcess);


        SingleOutputStreamOperator<String> reduce = window.reduce(
                new ReduceFunction<String>() {
            @Override
            public String reduce(String value1, String value2) throws Exception {
                return null;
            }
        });

        //aggregate
        SingleOutputStreamOperator<Integer> aggregate = window.aggregate(
                new AggregateFunction<String, A4_POJO, Integer>() {
                    @Override
                    public A4_POJO createAccumulator() {
                        return null;
                    }
                    @Override
                    public A4_POJO add(String value, A4_POJO accumulator) {
                        return null;
                    }
                    @Override
                    public Integer getResult(A4_POJO accumulator) {
                        return null;
                    }
                    //merge方法只有会话窗口才能用到,其他的用不到
                    @Override
                    public A4_POJO merge(A4_POJO a, A4_POJO b) {
                        return null;
                    }
                }
        );

        //简易版proces,没有上下文
        window.apply(new WindowFunction<String, Integer, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<String> input,
                              Collector<Integer> out) throws Exception {

            }
        });

        //待补充
//        window.sideOutputLateData();
//        window.allowedLateness();
//        window.trigger();




    }
}
