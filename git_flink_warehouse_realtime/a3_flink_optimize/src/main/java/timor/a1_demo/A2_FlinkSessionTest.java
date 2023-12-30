package timor.a1_demo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import timor.utils.TmKafkaUtils;

/**
 * @Author Timor
 * @Date 2023/12/29 10:56
 * @Version 1.0
 */
public class A2_FlinkSessionTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        KafkaSource<String> kafkaSource = TmKafkaUtils.getKafkaSource("flink_optimize", "aaaa");

        DataStreamSource<String> ds = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkasource");

        ds.print();

        env.execute();

    }


}
