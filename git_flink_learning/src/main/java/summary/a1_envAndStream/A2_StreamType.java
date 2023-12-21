package summary.a1_envAndStream;

import com.timor.flink.learning.dao.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.stringtemplate.v4.ST;

import java.util.Arrays;

/**
 * @Author Timor
 * @Date 2023/12/21 14:59
 * @Version 1.0
 */
public class A2_StreamType {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO----------------------------DataStream最基本的功能----------------------------------------
        DataStream<String> ds1 = env.fromCollection(Arrays.asList("a", "b"));
        DataStream<String> ds2 = env.fromCollection(Arrays.asList("a", "b"));
        //给流设置水位线，设置一次，下游通用
        //ds1.assignTimestampsAndWatermarks()

        //union算子,返回还是自己
        DataStream<String> union = ds1.union(ds2);
        //connect算子，返回ConnectedStreams类型
        ConnectedStreams<String, String> connect1 = ds1.connect(ds2);
        //keyby算子，返回KeyedStream类型
        KeyedStream<String, String> keyBy1 = ds1.keyBy(s -> s);
        //partitionCustom,分区算子,自定义分区，不过返回的是DataStream,不同于keyby
        //DataStream<String> stringDataStream = ds1.partitionCustom();

        //broadcast 当有参数时,是个广播流,当无参数时返回的还是DataStream
        DataStream<String> broadcast = ds1.broadcast();
        //assignTimestampsAndWatermarks设置水位线规则
        SingleOutputStreamOperator<String> water = ds1.assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());
        //join ,返回一个JoinedStreams,不知道和connect区别和union区别
        JoinedStreams<String, String> join = ds1.join(ds2);



        //AllWindowedStream<String, Window> windowAll = ds1.windowAll();
        //AllWindowedStream<String, GlobalWindow> countWindowAll = ds1.countWindowAll();

        //输出算子,有这类算子才调用
        //ds1.addSink()
        //这里可以直接调用flink官方提供的连接器直接sink,或者自己手写一个sink
        //ds1.sinkTo()


        //待补充
        DataStream<String> shuffle = ds1.shuffle();
        DataStream<String> rebalance = ds1.rebalance();
        DataStream<String> global = ds1.global();
        IterativeStream<String> iterate = ds1.iterate();



        //TODO----------------------------SingleOutputStreamOperator----------------------------------------
        //SingleOutputStreamOperator继承DataStream,新增了setParallelism方法，还有一些属性
        SingleOutputStreamOperator<String> single = ds1.map(String::toUpperCase);
        //获取流的算子设置并行度1
        single.setParallelism(1);
        //禁止串行
        single.disableChaining();
        //侧输出流
        single.getSideOutput(new OutputTag("out1"));
        //可以和哪些算子分到一个slot里,如果只有A算子设为1,那么其他算子不能和A在同一个slot,A独站一个slot
        single.slotSharingGroup("s1");
        //lamda推断不了类型的时候用returns
        single.returns(String.class);

        //TODO----------------------------OutPutStream----------------------------------------
        SingleOutputStreamOperator<WaterSensor> map = ds1.map(s -> new WaterSensor("A", 10L, 3));
        //定义流的标签时要放在算子外面，这样算子内所有的能调用
        OutputTag s1 = new OutputTag("s1stream", Types.POJO(WaterSensor.class));
        //分流侧输出流,返回的是主流，process和flatmap一样都有collector
        SingleOutputStreamOperator<WaterSensor> process = map.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                    ctx.output(s1, value);
                }
            });
        SideOutputDataStream sideOutputs1 = process.getSideOutput(s1);

        //TODO----------------------------DataStreamSource----------------------------------------
        //DataStreamSource 继承SingleOutput,只重写setParallelism方法,不能算一个单独类型，不需要记住
        DataStreamSource<String> ds3 = env.fromCollection(Arrays.asList("a", "b"));

        //TODO----------------------------KeyedStream----------------------------------------
        //KeyedStream是继承DataStream,算子:processs,intervalJoin,winodw,countWindow
        //小算子有sum,min,minBy,arreggate等返回个SingleOutputStreamOperator
        KeyedStream<String, String> keyBy = ds1.keyBy(s -> s);

        //TODO----------------------------ConnectedStreams----------------------------------------
        //ConnectedStreams 是一个新类,内部有2个属性，用来保存2个连接流
        //map,flatmap,process算子返回SingleOutputStreamOperator  keyby返回的是connected流
        ConnectedStreams<String, String> connect = ds1.connect(ds2);

        //keyBy.connect()



        //TODO----------------------------JoinedStream----------------------------------------
        //返回一个JoinedStreams,是一个新类,内部有2个属性，用来保存2个连接流,有where方法等
        JoinedStreams<String, String> join2 = ds1.join(ds2);

        //TODO----------------------------WindowedStream----------------------------------------
        //只有keyby的流才有window，其他的只能windowAll,DataStream只提供了WindowAll
        WindowedStream<String, String, TimeWindow> window = keyBy.window(TumblingEventTimeWindows.of(Time.seconds(10 * 1000L)));

        //AllWindowedStream<String, Window> windowAll = ds1.windowAll();
        //AllWindowedStream<String, GlobalWindow> countWindowAll = ds1.countWindowAll();


    }


}
