package com.timor.flink.learning.a3calculateoprater;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Title: A8_StreamBroadcast
 * @Package: com.timor.flink.learning.a3calculateoprater
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/23 19:16
 * @Version:1.0
 */
public class A8_StreamBroadcast {

    public static void main(String[] args) throws Exception {

        //广播流就是当流进行keyby分区的时候，每个分区的数据都会得到全部数据
        //广播流就是分区策略的一种
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> ds2 = env.fromElements(1, 2, 3,4,5,6,7);
        //转为广播流
        DataStream<Integer> broadcast = ds2.broadcast();
        KeyedStream<Integer, Integer> keyed = ds2.keyBy(x -> x);
        SingleOutputStreamOperator<Integer> map = keyed.map(x -> x);
        map.print();
        env.execute();
    }


}
