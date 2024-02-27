package a2_summary.a6_stateCoding;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @Author Timor
 * @Date 2024/2/26 22:59
 * @Version 1.0
 */
public class A0_非KeyedState {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.fromCollection(Arrays.asList("a", "b"));


        //TODO list,unionlist,broadcast

    }
}
