package scene;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lpc.utils.mysql.tools.A1_ConfigProperty;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Date;

/**
 * @Author Timor
 * @Date 2024/2/25 10:53
 * @Version 1.0
 */
public class A3_双流关联IntervalJoin {

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


        OutputTag<JSONObject> out_order = new OutputTag("mock_order",TypeExtractor.getForClass( JSONObject.class));
        OutputTag<JSONObject> out_cupon = new OutputTag("mock_cupon",TypeExtractor.getForClass( JSONObject.class));

        SingleOutputStreamOperator<JSONObject> process = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject data = jsonObject.getJSONObject("data");
                if ("mock_order".equals(jsonObject.getString("table"))) {
                    ctx.output(out_order,jsonObject.getJSONObject("data"));
                    System.out.println("抓到订单:"+data.getString("orderno"));

                }else if( "mock_cupon".equals(jsonObject.getString("table")) ){
                    ctx.output(out_cupon,jsonObject.getJSONObject("data"));
                    System.out.println("抓到cupon:"+data.getString("orderno")+data.getString("activity"));
                }
            }
        });

        SideOutputDataStream<JSONObject> orderDS = process.getSideOutput(out_order);
        SideOutputDataStream<JSONObject> cuponDS = process.getSideOutput(out_cupon);


        WatermarkStrategy<JSONObject> common_strategy = WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(
                        new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                Date create_time = element.getDate("create_time");
                                create_time.getTime();
                                return create_time.getTime();
                            }
                        });


        SingleOutputStreamOperator<JSONObject> oderStategyed = orderDS.assignTimestampsAndWatermarks(common_strategy);
        SingleOutputStreamOperator<JSONObject> cuponStategyed = cuponDS.assignTimestampsAndWatermarks(common_strategy);

        KeyedStream<JSONObject, String> ordernoKeyed = oderStategyed.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                String orderno = value.getString("orderno");
                System.out.println("order调用keyby:"+orderno);
                return orderno;
            }
        });

        KeyedStream<JSONObject, String> cuponKeyed = cuponStategyed.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                String orderno = value.getString("orderno");
                System.out.println("cupon调用keyby:"+orderno);
                return orderno;
            }
        });

        KeyedStream.IntervalJoin<JSONObject, JSONObject, String> interval = ordernoKeyed.intervalJoin(cuponKeyed);


        // TODO 注意当用了between,只能用事件事件nterval.inEventTime()，用interval.inProcessingTime()，会报错：Time-bounded stream joins are only supported in event time
        //按事件时间
       KeyedStream.IntervalJoin<JSONObject, JSONObject, String> processing = interval.inEventTime();
        //按到达时间
        //KeyedStream.IntervalJoin<JSONObject, JSONObject, String> processing = interval.inProcessingTime();


        OutputTag<JSONObject> late1 = new OutputTag("mock_order",TypeExtractor.getForClass( JSONObject.class));
        OutputTag<JSONObject> late2 = new OutputTag("mock_cupon",TypeExtractor.getForClass( JSONObject.class));

        //前后5S内的数据
        KeyedStream.IntervalJoined<JSONObject, JSONObject, String> between = processing
                .between(Time.seconds(2), Time.seconds(2))
                .sideOutputLeftLateData(late1) //左侧迟到数据放入测输出流
                .sideOutputRightLateData(late2)//右侧迟到数据放入测输出流
        ;
        //处理逻辑
        SingleOutputStreamOperator<String> process2 = between.process(new ProcessJoinFunction<JSONObject, JSONObject, String>() {
            @Override
            public void processElement(JSONObject left, JSONObject right, Context ctx, Collector<String> out) throws Exception {

                System.out.println("关联到了");
                out.collect(  "orderno:" +left.getString("orderno")+"  userid:"+left.getString("userid")+ "  activity:" + right.getString("activity") );

            }
        });
        process2.getSideOutput(late1).print();
        process2.getSideOutput(late2).print();

        process2.print();
        env.execute();


    }

}
