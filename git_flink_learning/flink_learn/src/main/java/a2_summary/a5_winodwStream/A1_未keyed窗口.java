package a2_summary.a5_winodwStream;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import a2_summary.a1_envAndStream.A4_POJO;

/**
 * @Title: A1_WindowAll
 * @Package: summary.a3_winodwStream
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/24 17:27
 * @Version:1.0
 */
public class A1_未keyed窗口 {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.fromElements("a", "b");

        //事件窗口，1000个事件滚动一次
        AllWindowedStream<String, GlobalWindow> win = ds.countWindowAll(1000);
        //滑动事件窗口,步长100
        AllWindowedStream<String, GlobalWindow> win2 = ds.countWindowAll(1000, 100);

        /**
         * 时间窗口7种实现类
         * slide滑动窗口，2种:事件时间，处理时间
         * tumbling滚动窗口，2种:事件时间，处理时间
         * session会话窗口，2种:事件时间，处理时间
         * global全局窗口，只有一个类
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

        AllWindowedStream<String, TimeWindow> wins = ds.windowAll(tmProcess);

        /**
         * 可调用的方法
         */
        //aggregate方法
        SingleOutputStreamOperator<Integer> aggregate = wins.aggregate(
                new AggregateFunction<String, A4_POJO, Integer>() {
             //开始调一次
             @Override
            public A4_POJO createAccumulator() {
                return null;
            }
            //窗口每个数据调一次
            @Override
            public A4_POJO add(String value, A4_POJO accumulator) {
                return null;
            }
            //最后调一次
            @Override
            public Integer getResult(A4_POJO accumulator) {
                return null;
            }
            //merge方法只有会话窗口才能用到,其他的用不到
            @Override
            public A4_POJO merge(A4_POJO a, A4_POJO b) {
                return null;
            }
        });

        //reduce方法
        SingleOutputStreamOperator<String> reduce = wins.reduce(
                new ReduceFunction<String>() {
            @Override
            public String reduce(String value1, String value2) throws Exception {
                return null;
            }
        });

        //apply 简易版process
        SingleOutputStreamOperator<Integer> apply = wins.apply(
                new AllWindowFunction<String, Integer, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<String> values, Collector<Integer> out)
                            throws Exception {
                    }
                });


//        //待定
//        AllWindowedStream<String, TimeWindow> windowedStream = wins.sideOutputLateData();
//        AllWindowedStream<String, TimeWindow> s = wins.allowedLateness();
//        AllWindowedStream<String, TimeWindow> trigger = wins.trigger();



    }
}
