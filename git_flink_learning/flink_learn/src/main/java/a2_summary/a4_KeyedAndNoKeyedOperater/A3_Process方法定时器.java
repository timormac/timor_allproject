package a2_summary.a4_KeyedAndNoKeyedOperater;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lpc.utils.mysql.tools.A1_ConfigProperty;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @Author Timor
 * @Date 2024/2/26 10:01
 * @Version 1.0
 */
public class A3_Process方法定时器 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(4);

        KafkaSource<String> kf= KafkaSource.<String>builder() //builder前面要指定范型方法
                .setBootstrapServers(A1_ConfigProperty.KAFKA_SERVER)
                .setGroupId("asdvf2222")
                .setTopics("maxwell")
                .setValueOnlyDeserializer(new SimpleStringSchema())//指定从kafka获取的数据的反序列化器
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();

        DataStreamSource<String> kafkaDS = env.fromSource(kf, WatermarkStrategy.noWatermarks(), "kafkasource");

        SingleOutputStreamOperator<JSONObject> map = kafkaDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {

                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject data = jsonObject.getJSONObject("data");
                return data;
            }
        });


        KeyedStream<JSONObject, String> keyBy = map.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getString("orderno");
            }
        });

        // TODO 定时器只有process方法有,process的Context和 Rich接口的GetRunTimeContext不是一个类型，无继承关系

        keyBy.process(new KeyedProcessFunction<String, JSONObject, Object>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<Object> out) throws Exception {

                TimerService timerService = ctx.timerService();

                //TODO 获取的是前面设置的时间规则时间,没设置事件事件，获取的是null
                Long timestamp = ctx.timestamp();

                //TODO 处理时间
                long currentTs = timerService.currentProcessingTime();


                //TODO 水位线延迟后时间
                //long currentWatermark = timerService.currentWatermark();
                // System.out.println(currentWatermark+":--currentWatermark");

                System.out.println(timestamp+":--timestamp");
                System.out.println(currentTs+":--currentTs");



                //TODO 事件时间定时器,注册指定之间触发的定时器：当前时间+触发间隔
               // timerService.registerEventTimeTimer(currentTs +10*1000L);

                //TODO 处理时间,注册指定之间触发的定时器：当前时间+触发间隔
                timerService.registerProcessingTimeTimer( currentTs +10*1000L);

//                 删除定时器： 处理时间、事件时间
//                        timerService.deleteEventTimeTimer();
//                        timerService.deleteProcessingTimeTimer();

                String format = DateFormatUtils.format(currentTs, "yyyy-MM-dd HH:mm:ss.SSS");
                System.out.println("现在时间是:"+format);

            }



            //TODO 用element流直接触发了,试试kafka流
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
                super.onTimer(timestamp, ctx, out);

                //这里的timestamp是上面定时器传入的参数，5s，而不是当前时间

                String format = DateFormatUtils.format(timestamp, "yyyy-MM-dd HH:mm:ss.SSS");
                System.out.println(timestamp);

                System.out.println("定时器触发时间："+format);



            }
        });

        env.execute();



    }
}
