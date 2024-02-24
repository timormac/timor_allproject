package a1_flink_learn.a7state;

import a1_flink_learn.dao.WaterSensor;
import a1_flink_learn.tools.UdfWaterSensorMap;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Title: A3_MapState
 * @Package: com.timor.flink.learning.a7state
 * @Description:
 * @Author: XXX
 * @Date: 2023/7/27 15:29
 * @Version:1.0
 */
public class A3_MapState {

    public static void main(String[] args) throws Exception {

        //需求:统计每种key，不同vs出现的次数
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

        SingleOutputStreamOperator<String> process = keyBy.process(new KeyedProcessFunction<String, WaterSensor, String>() {

            MapState<Integer, Integer> map;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                map = getRuntimeContext().getMapState(new MapStateDescriptor("map", Types.INT, Types.INT));

            }

            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx,
                                       Collector<String> out) throws Exception {

                Integer vc = value.getVc();
                if (map.contains(vc)) map.put(vc, map.get(vc) + 1);
                else map.put(vc, 1);

                out.collect("本次出现是" + vc + "出现次数为:" + map.get(vc));
 //                                vcCountMapState.get();          // 对本组的Map状态，根据key，获取value
//                                vcCountMapState.contains();     // 对本组的Map状态，判断key是否存在
//                                vcCountMapState.put(, );        // 对本组的Map状态，添加一个 键值对
//                                vcCountMapState.putAll();  // 对本组的Map状态，添加多个 键值对
//                                vcCountMapState.entries();      // 对本组的Map状态，获取所有键值对
//                                vcCountMapState.keys();         // 对本组的Map状态，获取所有键
//                                vcCountMapState.values();       // 对本组的Map状态，获取所有值
//                                vcCountMapState.remove();   // 对本组的Map状态，根据指定key，移除键值对
//                                vcCountMapState.isEmpty();      // 对本组的Map状态，判断是否为空
//                                vcCountMapState.iterator();     // 对本组的Map状态，获取迭代器
//                                vcCountMapState.clear();        // 对本组的Map状态，清空
            }
        });
        process.print();
        env.execute();


    }


}
