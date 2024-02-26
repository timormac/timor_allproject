package a2_summary.a3_StreamType;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @Title: A5_ConnectStream
 * @Package: summary.a3_StreamType
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/24 12:23
 * @Version:1.0
 */
public class A8_ConnectStream {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds1 = env.fromCollection(Arrays.asList("a", "b"));
        DataStream<String> ds2 = env.fromCollection(Arrays.asList("a", "b"));
        KeyedStream<String, String> keyed1 = ds1.keyBy(s -> "1");
        KeyedStream<String, String> keyed2 = ds2.keyBy(s -> "1");

        MapStateDescriptor<String, String> mapDescrip = new MapStateDescriptor<String, String>("broad", Types.STRING,Types.STRING);
        BroadcastStream<String> broadcast = ds1.broadcast(mapDescrip);

        /**ConnectStream是单独的类，不继承DataStream,方法都是自己重写的
         * DataStream的connect，参数只有2种:DataStream =>ConnectedStreams ,BroadcastStream =>BroadcastConnectedStream
         * KeyedStream的connect和DataStream一摸一样,返回值也一样
         */

        ConnectedStreams<String, String> dsConn = ds1.connect(ds2);
        BroadcastConnectedStream<String, String> broadConn = ds1.connect(broadcast);

        /** TODO  调用process方法
         *  ConnectedStreams调用process有2种:KeyedCoProcessFunction , CoProcessFunction
         *  BroadcastConnectedStream调用process有2种：KeyedBroadcastProcessFunction，BroadcastProcessFunction
         *  注意传KeyedBroadcastProcess时，必须2个流都是keye，不然报错
         */

        //一般都是先connect再keyb，先keyby再connect没有意义








    }
}
