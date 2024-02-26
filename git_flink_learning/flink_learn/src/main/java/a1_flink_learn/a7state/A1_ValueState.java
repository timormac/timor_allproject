package a1_flink_learn.a7state;

import a1_flink_learn.dao.WaterSensor;
import a1_flink_learn.tools.UdfWaterSensorMap;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Title: A1_KeyedValueState
 * @Package: com.timor.flink.learning.a7state
 * @Description:
 * @Author: XXX
 * @Date: 2023/7/27 13:49
 * @Version:1.0
 */
public class A1_ValueState {

    public static void main(String[] args) throws Exception {

        //需求：对于同一个key，前后2个vc的值相差10以上，则报警

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

            //状态机制必须用ValueState，并且在open里面初始化，不然会报错，应该是类加载顺序的问题
            //不能用普通的int定义，不然不同key的会共用一个，因为前面设置的setParallelism是1
            ValueState<Integer> vcState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //固定初始化写法，第一参数name当，定义多个状态时用来区分的，第二个参数是范型内的类型
                //getRuntimeContext()是KeyedProcessFunction继承的AbstractRichFunction中的方法,获取运营时上下文
                //查看ValueStateDescriptor的构造器有3种，有一种是 TypeInformation，在Tpyes中有很多final枚举
                vcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("vcState", Types.INT));

                //如果是自定义格式,用TypeExtractor.getForClasss(),只要类实现了可序列化就行
                ValueStateDescriptor<JSONObject> vcState = new ValueStateDescriptor<>("vcState", TypeExtractor.getForClass(JSONObject.class));

            }

            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {

                //lastVcState.value();  // 取出 本组 值状态 的数据
                //lastVcState.update(); // 更新 本组 值状态 的数据
                //lastVcState.clear();  // 清除 本组 值状态 的数据
                System.out.println("当前vcState：" + vcState.value());

                //初始化时返回的vcState的值是nul，要处理，不然下面if判断会报错
                int lastVc = vcState.value() == null ? 0 : vcState.value();

                int vc = value.getVc();

                if (Math.abs(lastVc - vc) > 10) {
                    out.collect("当前vc是：" + vc + "上一个vc是" + lastVc);
                }
                vcState.update(vc);

            }
        });

        process.print();

        env.execute();

    }

}
