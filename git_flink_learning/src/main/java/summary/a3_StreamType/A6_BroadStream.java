package summary.a3_StreamType;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @Title: BroadStream
 * @Package: summary.a3_StreamType
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/24 12:23
 * @Version:1.0
 */
public class A6_BroadStream {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds1 = env.fromCollection(Arrays.asList("a", "b"));
        DataStream<String> ds2 = env.fromCollection(Arrays.asList("a", "b"));

        /**
         * 广播流必须传参一个MapStateDescriptor，这个是一个状态，会被广播到下游所有并行度
         * 当有参数时,返回的是BroadcastStream,当无参数时返回的还是DataStream
         */
        //有参数时返回的还是DataStream
        MapStateDescriptor<String, String> mapDescrip = new MapStateDescriptor<String, String>("broad", Types.STRING,Types.STRING);
        BroadcastStream<String> broadcast = ds1.broadcast(mapDescrip);

        //不传参,返回的是DataStream流
        DataStream<String> broad2 = ds1.broadcast();


    }
}
