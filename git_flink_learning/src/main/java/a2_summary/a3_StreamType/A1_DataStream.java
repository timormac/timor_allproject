package a2_summary.a3_StreamType;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Arrays;

/**
 * @Title: A1_DataStream
 * @Package: summary.a2_StreamType
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/24 11:17
 * @Version:1.0
 */
public class A1_DataStream {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds1 = env.fromCollection(Arrays.asList("a", "b"));
        DataStream<String> ds2 = env.fromCollection(Arrays.asList("a", "b"));

        //给流设置水位线，设置一次，下游通用
        SingleOutputStreamOperator<String> water = ds1.assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());

        //lamda无法推断时，用这个类型提示
        water.returns(Types.POJO(String.class));

        //partitionCustom,分区算子,自定义分区，不过返回的是DataStream,不同于keyby
        //DataStream<String> stringDataStream = ds1.partitionCustom();

        //输出算子:addSink和SinkTo
        ds1.addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                SinkFunction.super.invoke(value, context);
            }
        });

        //这里可以直接调用flink官方提供的连接器直接sink,或者自己手写一个sink
        ds1.sinkTo(  KafkaSink.<String>builder().build() );

        /**
         * 常见的物理分区策略有：随机分配（Random）、轮询分配（Round-Robin）、重缩放（Rescale）和广播（Broadcast）
         * 分区策略是指同一个消息源，按什么方式发送给并行度,比如2个并行度，如果按random，运气不好可能全发给2线程
         * random策略通过 ds.shuffle()来调用
         * 轮询策略通过ds.rebalance()来调用
         * 重缩放 ds1.rescale(),具体看文档，这个不实用
         * 广播策略通过ds.broadcast()调用，所有的消息发送给所有并行度，重复数据
         * 所有流汇聚都下游某单个节点ds.global()，和setparallm(1)有区别的。还是多并行度，只有一个有数据
         */
        DataStream<String> shuffle = ds1.shuffle();
        DataStream<String> rebalance = ds1.rebalance();
        DataStream<String> broadcast1 = ds1.broadcast();
        DataStream<String> global = ds1.global();
        DataStream<String> rescale = ds1.rescale();

        //这2个不知道干什么用的
        IterativeStream<String> iterate = ds1.iterate();

        //TODO ----------------------获取其他形式流------------------------
        //union算子,返回的还是DataStream
        DataStream<String> union = ds1.union(ds2);

        //connect算子，返回ConnectedStreams类型
        ConnectedStreams<String, String> connect1 = ds1.connect(ds2);

        //keyby算子，返回KeyedStream类型
        KeyedStream<String, String> keyBy1 = ds1.keyBy(s -> s);

        //broadcast 当有参数时,是个广播流,当无参数时返回的还是DataStream
        MapStateDescriptor<String, String> mapDescrip = new MapStateDescriptor<String, String>("broad", Types.STRING,Types.STRING);
        BroadcastStream<String> broadcast = ds1.broadcast(mapDescrip);
        
        //不传参,返回的是DataStream流
        DataStream<String> broadcast3 = ds1.broadcast();

        //join ,返回一个JoinedStreams,不知道和connect区别和union区别
        JoinedStreams<String, String> join = ds1.join(ds2);



        //AllWindowedStream<String, Window> windowAll = ds1.windowAll();
        //AllWindowedStream<String, GlobalWindow> countWindowAll = ds1.countWindowAll();





    }
}
