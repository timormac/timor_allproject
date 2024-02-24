package a2_summary.a4_operatorInterface;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @Author Timor
 * @Date 2023/12/21 16:19
 * @Version 1.0
 */
public class A1_RichFunciton {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds1 = env.fromCollection(Arrays.asList("a", "b"));

    }


}
