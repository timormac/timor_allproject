package a1_flink_learn.a7state;

import a1_flink_learn.dao.WaterSensor;
import a1_flink_learn.tools.UdfWaterSensorMap;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Title: A6_TTL
 * @Package: com.timor.flink.learning.a7state
 * @Description:
 * @Author: XXX
 * @Date: 2023/7/28 15:34
 * @Version:1.0
 */
public class A6_TTL {

    public static void main(String[] args) {
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

        keyBy.process(new KeyedProcessFunction<String, WaterSensor, String>() {

            ValueState<Integer> value;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                // TODO 1.创建 StateTtlConfig
                StateTtlConfig stateTtlConfig = StateTtlConfig
                        .newBuilder(Time.seconds(5)) // 过期时间5s
//                      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 状态 创建和写入（更新） 更新 过期时间
                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite) // 状态 读取、创建和写入（更新） 更新 过期时间
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // 不返回过期的状态值
                        .build();

                // TODO 2.状态描述器 启用 TTL
                ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("lastVcState", Types.INT);
                stateDescriptor.enableTimeToLive(stateTtlConfig);


                value = getRuntimeContext().getState(stateDescriptor);


            }

            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx,
                                       Collector<String> out) throws Exception {

            }
        });




    }
}
