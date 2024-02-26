package a2_summary.a5_winodwStream;

import com.alibaba.fastjson.JSONObject;
import lpc.utils.mysql.tools.A1_ConfigProperty;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * @Author Timor
 * @Date 2024/2/26 11:54
 * @Version 1.0
 */
public class B2_KeyBy计数窗口demo {
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

        KeyedStream<JSONObject, String> keyby = map.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getString("userid");
            }
        });

        //TODO keyby后计数窗口,每个key单独一个窗口
        WindowedStream<JSONObject, String, GlobalWindow> wins = keyby.countWindow(3);

        SingleOutputStreamOperator<Object> process = wins.process(new ProcessWindowFunction<JSONObject, Object, String, GlobalWindow>() {
            @Override
            public void process(String s, Context context, Iterable<JSONObject> elements, Collector<Object> out) throws Exception {
                System.out.println("触发窗口");

                for (JSONObject element : elements) {
                    String orderno = element.getString("orderno");
                    String userid = element.getString("userid");
                    out.collect(orderno + "<>" +userid);
                }
            }
        });

        process.print();

        env.execute();

    }
}
