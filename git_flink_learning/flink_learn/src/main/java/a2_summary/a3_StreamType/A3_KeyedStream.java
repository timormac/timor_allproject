package a2_summary.a3_StreamType;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @Title: A3_Keyed
 * @Package: summary.a3_StreamType
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/24 12:22
 * @Version:1.0
 */
public class A3_KeyedStream {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO----------------------------DataStream最基本的功能----------------------------------------
        DataStream<String> ds1 = env.fromCollection(Arrays.asList("a", "b"));
        DataStream<String> ds2 = env.fromCollection(Arrays.asList("a", "b"));
        KeyedStream<String, String> keyBy = ds1.keyBy(s -> s);
        KeyedStream<String, String> keyby2 = ds2.keyBy(s -> s);

        /**
         *KeyedStream是继承DataStream
         *自己新增了些方法:sum,min,minBy,arreggate等返回个SingleOutputStreamOperator
         * 还有个intervaljoin方法
         * 一般使用方法：先connect再keyb，或者2个keyby后的流connect
         */


        //intervaljoin 参数必须是keyedStream,返回值是个内部类，没法直接import
        KeyedStream.IntervalJoin<String, String, String> interval = keyBy.intervalJoin(keyby2);

        ConnectedStreams<String, String> connect = ds1.connect(ds2);

        //先connect，再keyby,或者2个keyby后的流connect
        ConnectedStreams<String, String> keyedConnect = connect.keyBy(s->"1",s->"2");

    }
}
