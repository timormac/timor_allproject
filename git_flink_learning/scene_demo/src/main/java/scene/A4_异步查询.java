package scene;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lpc.utils.mysql.tools.A1_ConfigProperty;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Supplier;


/**
 * @Author Timor
 * @Date 2024/2/25 14:32
 * @Version 1.0
 */
public class A4_异步查询 {

    public static void main(String[] args) {
        /**
         * 异步编程有点难,里面每太看懂
         */

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


        // 定义异步操作
        AsyncFunction<JSONObject, String> asyncFunction = new MockDBQuery();

        // 应用异步I/O操作
        DataStream<String> resultStream = AsyncDataStream.unorderedWait(
                orderDS,
                asyncFunction,
                1000, // 超时时间（毫秒）
                java.util.concurrent.TimeUnit.MILLISECONDS,
                20     // 最大异步请求数
        );


    }

}

class MockDBQuery extends RichAsyncFunction<JSONObject,String> {


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

    }

    @Override
    public void close() throws Exception {
        super.close();

    }

    //没太看懂,result没注册啊,程序怎么知道result代码是查询
    @Override
    public void asyncInvoke(JSONObject input, ResultFuture<String> resultFuture) throws Exception {
        // 发送异步请求，接收 future 结果,执行查询
        final Future<String> result = null;

        // 设置客户端完成请求后要执行的回调函数
        // 回调函数只是简单地把结果发给 future
        CompletableFuture<String> completableFuture = CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                try {
                    return result.get();
                } catch (InterruptedException | ExecutionException e) {
                    // 显示地处理异常。
                    return null;
                }
            }
        });

        completableFuture.thenAccept(new Consumer<String>() {
            @Override
            public void accept(String s) {
                //输出
                resultFuture.complete( Collections.singleton(s) );
            }
        });


    }

    @Override
    public void timeout(JSONObject input, ResultFuture<String> resultFuture) throws Exception {

    }


}
