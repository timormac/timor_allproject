package a2_summary.a5_winodwStream;

import com.alibaba.fastjson.JSONObject;
import lpc.utils.mysql.tools.A1_ConfigProperty;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * @Author Timor
 * @Date 2024/2/26 10:57
 * @Version 1.0
 */
public class B1_全局计数窗口Demo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(4);

        KafkaSource<String> kf= KafkaSource.<String>builder() //builder前面要指定范型方法
                .setBootstrapServers(A1_ConfigProperty.KAFKA_SERVER)
                .setGroupId("asdvf2222")
                .setTopics("maxwell")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())//指定从kafka获取的数据的反序列化器
                .build();

        DataStreamSource<String> ds = env.fromSource(kf, WatermarkStrategy.noWatermarks(), "kafkasource");

        SingleOutputStreamOperator<JSONObject> map = ds.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                JSONObject obj = JSONObject.parseObject(value);
                JSONObject data = obj.getJSONObject("data");
                return data;
            }
        });

        //TODO 执行countWindowAll后就变为并行度为1
        AllWindowedStream<JSONObject, GlobalWindow> w = map.countWindowAll(5L);

        SingleOutputStreamOperator<Object> process = w.process(new ProcessAllWindowFunction<JSONObject, Object, GlobalWindow>() {

            @Override
            public void process(Context context, Iterable<JSONObject> elements, Collector<Object> out) throws Exception {

                System.out.println("触发窗口");

                for (JSONObject element : elements) {
                    String orderno = element.getString("orderno");
                    out.collect(orderno);
                }

            }
        });

        //TODO 最后并行度还是变为4,并且out.collect会把数据发给print的4个并行度
        process.print();
        env.execute();


    }
}
