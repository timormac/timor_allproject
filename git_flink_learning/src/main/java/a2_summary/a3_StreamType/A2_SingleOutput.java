package a2_summary.a3_StreamType;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;

/**
 * @Title: A2_SingleOutput
 * @Package: summary.a3_StreamType
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/24 12:21
 * @Version:1.0
 */
public class A2_SingleOutput {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds1 = env.fromCollection(Arrays.asList("a", "b"));

        /**
         * singleOutputStreamOperator继承DataStream,
         * 自己新增了setParallelism方法，还有一些属性
         */
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

    }
}
