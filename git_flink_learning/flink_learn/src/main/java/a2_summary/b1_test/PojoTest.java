package a2_summary.b1_test;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import a2_summary.a1_envAndStream.A4_POJO;

import java.util.Arrays;

/**
 * @Title: PojoTest
 * @Package: summary.b1_test
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/24 12:07
 * @Version:1.0
 */
public class PojoTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds1 = env.fromCollection(Arrays.asList("a", "b"));

        OutputTag<A4_POJO> tag1 = new OutputTag<>("tag1",Types.POJO(A4_POJO.class) );

        SingleOutputStreamOperator<A4_POJO> map = ds1.process( new ProcessFunction<String, A4_POJO>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, A4_POJO>.Context ctx,
                                               Collector<A4_POJO> out) throws Exception {

                        ctx.output(tag1,new A4_POJO());
                    }
                }
        );

        SideOutputDataStream<A4_POJO> sideOutput = map.getSideOutput(tag1);


        sideOutput.print();

        env.execute();

    }
}
