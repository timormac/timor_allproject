package a1_flink_learn.a7state;

import a1_flink_learn.dao.WaterSensor;
import a1_flink_learn.tools.UdfWaterSensorMap;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Title: A8_BroadcastState
 * @Package: com.timor.flink.learning.a7state
 * @Description:
 * @Author: XXX
 * @Date: 2023/7/28 15:57
 * @Version:1.0
 */
public class A8_BroadcastState {
    public static void main(String[] args) throws Exception {

        //需求:设定一个阀值，全局通用，且可以实时更改的，如果水位超过阀值数据报警
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //数据流
        SingleOutputStreamOperator<WaterSensor> map = env
                .socketTextStream("localhost", 7777)
                .map(new UdfWaterSensorMap());


        //配置广播流
        MapStateDescriptor<String, Integer> broadcastMapState = new MapStateDescriptor<>("broadcast-state", Types.STRING, Types.INT);



        env.execute();

    }
}
