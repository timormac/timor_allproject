package a1_flink_learn.a7state;

import a1_flink_learn.dao.WaterSensor;
import a1_flink_learn.tools.UdfWaterSensorMap;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Title: A5_AggregateState
 * @Package: com.timor.flink.learning.a7state
 * @Description:
 * @Author: XXX
 * @Date: 2023/7/28 14:33
 * @Version:1.0
 */
public class A5_AggregateState {

    public static void main(String[] args) throws Exception {

        //需求计算平均v，aggregate和reduce区别就是有个中间，不用输入与输出相同
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> map = env
                .socketTextStream("localhost", 7777)
                .map(new UdfWaterSensorMap());

        WatermarkStrategy<WaterSensor> strategy = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                });

        SingleOutputStreamOperator<WaterSensor> mapStrategy = map.assignTimestampsAndWatermarks(strategy);
        KeyedStream<WaterSensor, String> keyBy = mapStrategy.keyBy(sensor -> sensor.getId());

        SingleOutputStreamOperator<String> process = keyBy.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {

                    //第一个范型是输入，第二范型是输出，中间的范型在open中写
                    AggregatingState<Integer,Double> agg;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        agg = getRuntimeContext().getAggregatingState(
                                new AggregatingStateDescriptor<Integer, Tuple2<Integer,Integer>, Double>(
                                        "agg",
                                        new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                                            @Override
                                            public Tuple2<Integer, Integer> createAccumulator() {
                                                return Tuple2.of(0,0);
                                            }

                                            @Override
                                            public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                                                return  Tuple2.of(  accumulator.f0+value , accumulator.f1+1 );
                                            }

                                            @Override
                                            public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                                return accumulator.f0*1D/accumulator.f1;
                                            }

                                            @Override
                                            public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                                                return null;
                                            }
                                        },
                                        Types.TUPLE(Types.INT,Types.INT)
                                )
                        );

                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx,
                                               Collector<String> out) throws Exception {

                        //虽然agg只能获取到结果值，但是merge中间数据是一直存在open方法中的，只是拿不到了
                        agg.add(value.getVc());

                        out.collect( agg.get().toString() );

                    }
                }
        );

        process.print();
        env.execute();

    }



}
