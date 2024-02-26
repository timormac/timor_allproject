package a2_summary.a3_StreamType;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Arrays;

/**
 * @Title: A7_JoinStream
 * @Package: summary.a3_StreamType
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/24 12:25
 * @Version:1.0
 */
public class A7_JoinStream {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds1 = env.fromCollection(Arrays.asList("a", "b"));
        DataStream<String> ds2 = env.fromCollection(Arrays.asList("a", "b"));


        JoinedStreams<String, String> join2 = ds1.join(ds2);
        /**
         * JoinedStreams,是一个新。内部有2个流属性
         * 固定写法,调where()后，返回一个类，只有一个equalTo方法，使用后只有一个window方法,window后才有apply方法
         */

        //TODO 固定写法
        join2.where(null)  //ds1的keyby
                .equalTo(null)//ds2的keyby
                .window(null) //窗口大小,inner join
                .apply(  (s1,s2)->s1+s2); //处理逻辑


        //调用方法后返回新类
        JoinedStreams<String, String>.Where<Object> where = join2.where(null);
        JoinedStreams<String, String>.Where<Object>.EqualTo equalTo = where.equalTo(null);
        JoinedStreams.WithWindow<String, String, Object, TimeWindow> window = equalTo.window(TumblingProcessingTimeWindows.of(Time.minutes(1)));

    }
}
