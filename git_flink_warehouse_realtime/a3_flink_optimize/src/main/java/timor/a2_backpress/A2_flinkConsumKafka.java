package timor.a2_backpress;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import timor.utils.A0_ConnectPool;
import timor.utils.TmKafkaUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

/**
 * @Author Timor
 * @Date 2023/12/18 20:53
 * @Version 1.0
 */
public class A2_flinkConsumKafka {

    public static void main(String[] args) throws Exception {

        //模拟下游消费者速度慢的背压情况查看

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);

        //设置不能串链,方便看背压
        env.disableOperatorChaining();

        KafkaSource<String> kafkaSource = TmKafkaUtils.getKafkaSource("flink_optimize", "optimize2");

        DataStreamSource<String> ds = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkasource");

        SingleOutputStreamOperator<JSONObject> map = ds.map(JSON::parseObject);


        KeyedStream<JSONObject, String> keyed = map.keyBy(s -> s.getString("id"));

        SingleOutputStreamOperator<Tuple4<String, Integer, Double, Double>> process = keyed.process(new KeyedProcessFunction<String, JSONObject, Tuple4<String, Integer, Double, Double>>() {

            int count;
            double sum;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<Tuple4<String, Integer, Double, Double>> out) throws Exception {

                String key = ctx.getCurrentKey();
                count += value.getDouble("money");
                sum += 1;
                out.collect(new Tuple4<String, Integer, Double, Double>(key, count, sum,value.getDouble("money")));
                Thread.sleep(10);
            }
        });



        process.addSink(new RichSinkFunction<Tuple4<String, Integer, Double, Double>>() {
            Connection poolConnect;
            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("------------------------------创建线程池-------------------------");
                poolConnect = A0_ConnectPool.getPoolConnect();
            }

            @Override
            public void invoke(Tuple4<String, Integer, Double, Double> value, Context context) throws Exception {

                String sql = "insert into flink_optmize(id,countnum,sumnum,money,create_time) values(?,?,?,?,?)";
                PreparedStatement pst = poolConnect.prepareStatement(sql);
                pst.setString(1,value.f0);
                pst.setInt(2,value.f1);
                pst.setDouble(3,value.f2);
                pst.setDouble(4,value.f3);
                //注意这里要timestamp,别用setdate,不然没有时分秒
                Timestamp timestamp = new Timestamp( System.currentTimeMillis() );
                pst.setTimestamp(5,timestamp);
                pst.execute();
            }

        });

        env.execute();

    }



}
