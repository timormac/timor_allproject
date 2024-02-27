package a2_summary.a2_SourceAndSink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;
import java.util.Arrays;

/**
 * @Author Timor
 * @Date 2023/12/21 16:39
 * @Version 1.0
 */
public class A4_KafkaSink {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.fromCollection(Arrays.asList("a", "b"));


        /**
         * Kafka Sink:
         * TODO 注意：如果要使用 精准一次 写入Kafka，需要满足以下条件，缺一不可
         * 1、开启checkpoint（后续介绍）
         * 2、设置事务前缀
         * 3、设置事务超时时间：   checkpoint间隔 <  事务超时时间  < max的15分钟
         */
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                // 指定 kafka 的地址和端口
                .setBootstrapServers("project1:9092,project2:9092,project3:9092")
                // 指定序列化器：指定Topic名称、具体的序列化
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("ws")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                // 写到kafka的一致性级别： 精准一次、至少一次
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 如果是精准一次，必须设置 事务的前缀
                .setTransactionalIdPrefix("atguigu-")
                // 如果是精准一次，必须设置 事务超时时间: 大于checkpoint间隔，小于 max 15分钟
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10*60*1000+"")
                .build();

        ds.sinkTo(kafkaSink);




        //TODO flink去消费 被 两阶段提交的 topic，设置隔离级别
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setGroupId("atguigu")
                .setTopics("ws")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest())
                // TODO 作为 下游的消费者，要设置 事务的隔离级别 = 读已提交
                .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .build();


        env.fromSource(kafkaSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3)), "kafkasource")
                .print();



    }
}
