package a1_flink_learn.a7state;

import a1_flink_learn.dao.WaterSensor;
import a1_flink_learn.tools.UdfWaterSensorMap;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;

/**
 * @Title: A2_LIstState
 * @Package: com.timor.flink.learning.a7state
 * @Description:
 * @Author: XXX
 * @Date: 2023/7/27 14:47
 * @Version:1.0
 */
public class A2_LIstState {

    public static void main(String[] args) throws Exception {

        //需求:每个key获取最大的3个vc

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> map = env
                .socketTextStream("localhost", 7777)
                .map(new UdfWaterSensorMap());


        WatermarkStrategy<WaterSensor> strategy = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                });

        SingleOutputStreamOperator<WaterSensor> mapWithStrategy = map.assignTimestampsAndWatermarks(strategy);

        KeyedStream<WaterSensor, String> keyBy = map.keyBy(sensor -> sensor.getId());

        SingleOutputStreamOperator<String> process = keyBy.process(new KeyedProcessFunction<String, WaterSensor, String>() {

            ListState<Integer> listVc;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                //调用的是getListState()
                listVc = getRuntimeContext().getListState(new ListStateDescriptor("listVC", Class.forName("java.lang.Integer")));
            }

            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx,
                                       Collector<String> out) throws Exception {

                //因为ListState没有sort排序，所以拷贝一个
                Iterable<Integer> iter = listVc.get();

                ArrayList<Integer> tempList = new ArrayList<Integer>();

                for (Integer i : iter) {
                    tempList.add(i);
                }

                Integer vc = value.getVc();

                if (tempList.size() == 3) {
                    tempList.sort((o1, o2) -> o2 - o1);
                    if (vc > tempList.get(2)) {
                        tempList.remove(2);
                        tempList.add(vc);
                    }
                } else {
                    tempList.add(vc);
                }

//                                vcListState.get();            //取出 list状态 本组的数据，是一个Iterable
//                                vcListState.add();            // 向 list状态 本组 添加一个元素
//                                vcListState.addAll();         // 向 list状态 本组 添加多个元素
//                                vcListState.update();         // 更新 list状态 本组数据（覆盖）
//                                vcListState.clear();          // 清空List状态 本组数据
                listVc.update(tempList);

                out.collect( tempList.toString() );

            }
        });
        process.print();

        env.execute();

    }
}
