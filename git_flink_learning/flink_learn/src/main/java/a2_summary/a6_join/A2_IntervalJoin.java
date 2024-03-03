package a2_summary.a6_join;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author Timor
 * @Date 2024/2/29 9:30
 * @Version 1.0
 */
public class A2_IntervalJoin {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds1 = env.fromElements("a");
        DataStreamSource<String> ds2 = env.fromElements("b");


        /**
         * 1 intervalJoin只能用在keyby后的2个流
         * 2 能获取左右2个迟到数据到侧流
         * 3 使用between方法，必须是事件时间：interval.inEventTime();
         * 4 传入processJoinFunction,才能获取SingleOutputStream
         *
         */

        KeyedStream<String, String> keyBy1 = ds1.keyBy(s -> s);
        KeyedStream<String, String> keyBy2 = ds1.keyBy(s -> s);

        //TODO intervaljoin这个流。源码内部存2个keyed流
        KeyedStream.IntervalJoin<String, String, String> interval = keyBy1.intervalJoin(keyBy2);
        KeyedStream.IntervalJoin<String, String, String> event = interval.inEventTime();
        KeyedStream.IntervalJoined<String, String, String> joined = event.between(Time.seconds(5), Time.minutes(5));
        SingleOutputStreamOperator<Object> process = joined
                .sideOutputLeftLateData(null) //左侧迟到数据放入测输出流
                .sideOutputRightLateData(null)//右侧迟到数据放入测输出流
                .process(null);//传入一个processJoinFunction

        process.print();

    }
}
