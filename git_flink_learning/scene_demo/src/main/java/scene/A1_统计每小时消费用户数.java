package scene;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lpc.utils.mysql.tools.A1_ConfigProperty;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author Timor
 * @Date 2024/2/24 23:55
 * @Version 1.0
 */
public class A1_统计每小时消费用户数 {
    public static void main(String[] args) throws Exception {

        /**
         * 统计每整数小时用户总数
         */

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(4);
        //从kafka获取
        KafkaSource<String> kf= KafkaSource.<String>builder() //builder前面要指定范型方法
                .setBootstrapServers(A1_ConfigProperty.KAFKA_SERVER)
                .setGroupId("dim_consumer")
                .setTopics("maxwell")
                .setValueOnlyDeserializer(new SimpleStringSchema())//指定从kafka获取的数据的反序列化器
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();

        DataStreamSource<String> kafkaDS = env.fromSource(kf, WatermarkStrategy.noWatermarks(), "kafkasource");

        SingleOutputStreamOperator<JSONObject> orderDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                if ("mock_order".equals(jsonObject.getString("table"))) {
                    out.collect(jsonObject.getJSONObject("data"));
                }
            }
        });


        KeyedStream<JSONObject, String> keyed = orderDS.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getString("userid");
            }
        });

        SingleOutputStreamOperator<String> process = keyed.process(new KeyedProcessFunction<String, JSONObject, String>() {
            MapState<String, Integer> hourMap;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                hourMap = getRuntimeContext().getMapState(new MapStateDescriptor<>("hour", Types.STRING, Types.INT));
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                String create_time = value.getString("create_time");
                String hour = create_time.substring(0, 13);

                if (!hourMap.contains(hour)) {
                    hourMap.put(hour, 1);
                    out.collect(ctx.getCurrentKey());
                } else {
                    hourMap.put(hour, hourMap.get(hour) + 1);
                }

            }
        });

        SingleOutputStreamOperator<Integer> total = process
                .keyBy (new KeySelector<String, Integer>() {
                    @Override
                    public Integer getKey(String value) throws Exception {
                        return 1;
                    }
                }).map( new RichMapFunction<String, Integer>() {

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
