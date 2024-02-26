package scene;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lpc.utils.mysql.tools.A1_ConfigProperty;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

/**
 * @Author Timor
 * @Date 2024/2/24 21:39
 * @Version 1.0
 */
public class A2_双流JoinConnect实现 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(4);
        //从kafka获取
        KafkaSource<String> kf= KafkaSource.<String>builder() //builder前面要指定范型方法
                .setBootstrapServers(A1_ConfigProperty.KAFKA_SERVER)
                .setGroupId("dim_consumer")
                .setTopics("maxwell")
                .setValueOnlyDeserializer(new SimpleStringSchema())//指定从kafka获取的数据的反序列化器
                .setStartingOffsets(OffsetsInitializer.latest())  //设置消费起始offset
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

        SingleOutputStreamOperator<JSONObject> cuponDS = kafkaDS.flatMap((String str, Collector<JSONObject> out) -> {
            JSONObject jsonObject = JSON.parseObject(str);
            if ("mock_cupon".equals(jsonObject.getString("table"))) {
                out.collect(jsonObject.getJSONObject("data"));
            }
        }).returns(TypeExtractor.getForClass( JSONObject.class));


        /**
         * keyed后,Connect流实现2流join，自己管理状态
         */
        KeyedStream<JSONObject, String> keyedOrderno = orderDS.keyBy(s->s.getString("orderno"));
        KeyedStream<JSONObject, String> keyedCupon = cuponDS.keyBy(s -> s.getString("orderno"));
        ConnectedStreams<JSONObject, JSONObject> connect = keyedCupon.connect(keyedOrderno);

        //这步实现双流join
        SingleOutputStreamOperator<Object> process = connect.process(new KeyedCoProcessFunction<Object, JSONObject, JSONObject, Object>() {

            MapState<String, JSONObject> cuponMap;
            MapState<String, JSONObject> orderMap;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                StateTtlConfig stateTtlConfig = StateTtlConfig
                        .newBuilder(Time.seconds(10)) // 过期时间5s
//                      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 状态 创建和写入（更新） 更新 过期时间
                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite) // 状态 读取、创建和写入（更新） 更新 过期时间
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // 不返回过期的状态值
                        .build();

                MapStateDescriptor cuponState = new MapStateDescriptor("cuponMap", Types.INT, TypeExtractor.getForClass(JSONObject.class));
                MapStateDescriptor orderState = new MapStateDescriptor("orderMap", Types.STRING, TypeExtractor.getForClass(JSONObject.class));

                cuponState.enableTimeToLive(stateTtlConfig);
                orderState.enableTimeToLive(stateTtlConfig);

                this.cuponMap = getRuntimeContext().getMapState(cuponState);
                this.orderMap = getRuntimeContext().getMapState(orderState);

            }

            @Override
            public void processElement1(JSONObject value, Context ctx, Collector<Object> out) throws Exception {

                String orderno = value.getString("orderno");
                String activity = value.getString("activity");
                String cuponno = value.getString("cuponno");
                cuponMap.put(orderno, value);

                System.out.println("优惠券："+ cuponno );

                if (orderMap.contains(orderno)) {
                    System.out.println("关联到了");
                    out.collect("orderno: " + orderno + " activity: " + activity + " userid: " + orderMap.get(orderno).getString("userid"));
                }

            }

            @Override
            public void processElement2(JSONObject value, Context ctx, Collector<Object> out) throws Exception {
                String orderno = value.getString("orderno");
                String userid = value.getString("userid");

                System.out.println( "订单来:"+orderno );
                orderMap.put(orderno, value);

                if (cuponMap.contains(orderno)) {
                    JSONObject jsonObject = cuponMap.get(orderno);
                    out.collect("orderno: " + orderno + " activity: " + cuponMap.get(orderno).getString("activity") + " userid: " + userid);
                }
            }
        });


        //  keyedCupon.intervalJoin()

        process.print();


        env.execute();




    }

}
