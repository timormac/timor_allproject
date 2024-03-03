package a2_summary.a4_KeyedAndNoKeyedOperater;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.Arrays;

/**
 * @Author Timor
 * @Date 2024/2/25 11:07
 * @Version 1.0
 */
public class A1_Keyed算子 {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStream<String> ds1 = env.fromCollection(Arrays.asList("a", "b"));
        DataStream<String> ds2 = env.fromCollection(Arrays.asList("a", "b"));

        KeyedStream<String, String> keyBy1 = ds1.keyBy(s -> s);
        KeyedStream<String, String> keyBy2 = ds2.keyBy(s -> s);


        /**
         * keyedby自己独有的方法
         */
        //TODO intervaljoin这个流。源码内部存2个keyed流
        KeyedStream.IntervalJoin<String, String, String> interval = keyBy1.intervalJoin(keyBy2);
        KeyedStream.IntervalJoined<String, String, String> joined = interval.between(Time.seconds(5), Time.minutes(5));

        joined.sideOutputLeftLateData(null) //左侧迟到数据放入测输出流
                .sideOutputRightLateData(null)//左侧迟到数据放入测输出流
                .process(null);//关联到的数据，放入process


        WindowedStream<String, String, Window> window = keyBy1.window(null);

        keyBy1.countWindow(1,2);


        ConnectedStreams<String, String> connect = keyBy1.connect(keyBy2);


        /**
         * 继承DataStream未修改的方法
         */
        //flatmap,map,filter,assignTimestampsAndWatermark,keyby
        keyBy1.coGroup(null);
        keyBy1.join(null);
        keyBy1.windowAll(null);
        keyBy1.countWindowAll(1,2);


        /**
         * 继承DataStream重写的方法
         */

        //也不算重写,参数类型变了
        //keyBy1.process(null);










    }

}
