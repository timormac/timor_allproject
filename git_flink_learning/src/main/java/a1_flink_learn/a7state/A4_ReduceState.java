package a1_flink_learn.a7state;

import a1_flink_learn.dao.WaterSensor;
import a1_flink_learn.tools.UdfWaterSensorMap;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Title: A4_ReduceState
 * @Package: com.timor.flink.learning.a7state
 * @Description:
 * @Author: XXX
 * @Date: 2023/7/28 14:25
 * @Version:1.0
 */
public class A4_ReduceState {

    public static void main(String[] args) throws Exception {

        //需求:统计每种key，vs的总和
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

        SingleOutputStreamOperator<String> process = keyBy
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

            ReducingState<Integer> reduce;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                reduce = getRuntimeContext().getReducingState(
                        new ReducingStateDescriptor<Integer>(
                                "reduce",
                                new ReduceFunction<Integer>() {
                                    @Override
                                    public Integer reduce(Integer value1, Integer value2) throws Exception {
                                        return value1+value2 ;
                                    }
                                },
                                Types.INT
                        )
                );

            }

            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx,
                                       Collector<String> out) throws Exception {

                reduce.add( value.getVc());
                out.collect( reduce.get().toString() );

            }
        });

        process.print();
        env.execute();

    }
}
