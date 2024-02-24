package a2_summary.a3_StreamType;

import a1_flink_learn.dao.WaterSensor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;

/**
 * @Title: OutPutStream
 * @Package: summary.a3_StreamType
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/24 12:24
 * @Version:1.0
 */
public class A5_OutPutStream {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds1 = env.fromCollection(Arrays.asList("a", "b"));
        SingleOutputStreamOperator<WaterSensor> map = ds1.map(s -> new WaterSensor("A", 10L, 3));

        //定义流的标签时要放在算子外面，这样算子内所有的能调用
        OutputTag<WaterSensor> s1 = new OutputTag<>("s1stream", Types.POJO(WaterSensor.class));

        //分流侧输出流,返回的是主流，process和flatmap一样都有collector
        SingleOutputStreamOperator<WaterSensor> process = map.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                ctx.output(s1, value);
            }
        });

        SideOutputDataStream<WaterSensor> sideOutput = process.getSideOutput(s1);


    }
}
