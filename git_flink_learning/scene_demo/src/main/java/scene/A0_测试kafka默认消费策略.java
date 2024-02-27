package scene;

import lpc.utils.mysql.tools.A1_ConfigProperty;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @Author Timor
 * @Date 2024/2/25 22:25
 * @Version 1.0
 */
public class A0_测试kafka默认消费策略 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(4);

        //TODO 默认策略 ,从未消费过从最早消费,消费过是从已有偏移量消费,并且不会提交偏移量
        // 不需要手动设置chekpoint提交偏移量,当发现env设置了checkpoint,会自动通过checkpoint提交偏移量
        KafkaSource<String> kf= KafkaSource.<String>builder() //builder前面要指定范型方法
                .setBootstrapServers(A1_ConfigProperty.KAFKA_SERVER)
                .setGroupId("asdvf2222")
                .setTopics("maxwell")
                .setValueOnlyDeserializer(new SimpleStringSchema())//指定从kafka获取的数据的反序列化器
                .build();
        DataStreamSource<String> kafkaDS = env.fromSource(kf, WatermarkStrategy.noWatermarks(), "kafkasource");


        //TODO 默认策略 若从未消费从最新  ,是从已有偏移量消费
//        Properties properties = new Properties();
//        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, A1_ConfigProperty.KAFKA_SERVER);
//        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"newTest");
//        FlinkKafkaConsumer<String> kafka = new FlinkKafkaConsumer("maxwell", new SimpleStringSchema(), properties);
//        kafka.assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());
//        DataStreamSource<String> kafkaDS = env.addSource(kafka);

        KeyedStream<String, Integer> keyed = kafkaDS.keyBy(new KeySelector<String, Integer>() {
            @Override
            public Integer getKey(String value) throws Exception {
                return 1;
            }
        });

        SingleOutputStreamOperator<Integer> total = keyed.map(new RichMapFunction<String, Integer>() {

            ValueState<Integer> total;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                total = getRuntimeContext().getState(new ValueStateDescriptor<>("total", Types.INT));
            }

            @Override
            public Integer map(String value) throws Exception {

                if (total.value() == null) {
                    total.update(1);
                } else {
                    total.update(total.value() + 1);
                }
                return total.value();
            }
        });
        total.print();

        env.execute();




    }
}
