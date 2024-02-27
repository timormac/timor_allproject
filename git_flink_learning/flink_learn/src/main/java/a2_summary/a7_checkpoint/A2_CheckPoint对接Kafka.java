package a2_summary.a7_checkpoint;

import lpc.utils.mysql.tools.A1_ConfigProperty;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @Author Timor
 * @Date 2024/2/26 22:45
 * @Version 1.0
 */
public class A2_CheckPoint对接Kafka {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(4);

        //TODO 默认策略 ,从未消费过从最早消费,消费过是从已有偏移量消费,并且不会提交偏移量
        // 不需要手动设置chekpoint提交偏移量,当发现env设置了checkpoint,会自动通过checkpoint提交偏移量
        KafkaSource<String> kf= KafkaSource.<String>builder().build();
        DataStreamSource<String> kafkaDS = env.fromSource(kf, WatermarkStrategy.noWatermarks(), "kafkasource");


        //TODO  KafkaConcumer默认策略 若从未消费从最新,是从已有偏移量消费。
        // 需要手动设置checkpoint提交偏移量

        Properties properties = new Properties();
        //将自动提交关闭
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        FlinkKafkaConsumer kafka = new FlinkKafkaConsumer("aa", new SimpleStringSchema(), properties);

        //设置通过checkpoint提交偏移量
        kafka.setCommitOffsetsOnCheckpoints(true)  ;

        DataStreamSource dataStreamSource = env.addSource(kafka);



    }
}
