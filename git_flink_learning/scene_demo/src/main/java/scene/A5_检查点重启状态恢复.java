package scene;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lpc.utils.mysql.tools.A1_ConfigProperty;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @Author Timor
 * @Date 2024/2/25 20:34
 * @Version 1.0
 */
public class A5_检查点重启状态恢复 {
    public static void main(String[] args) throws Exception {
        /**
         * 统计每整数小时用户总数
         */

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(4);

        // TODO 检查点常用配置
        System.setProperty("HADOOP_USER_NAME", "lpc");
        // 1、启用检查点: 默认是barrier对齐的，周期为5s, 精准一次
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 2、指定检查点的存储位置
        checkpointConfig.setCheckpointStorage("hdfs://project1:8020/flink_cep");
        // 3、checkpoint的超时时间: 默认10分钟
        checkpointConfig.setCheckpointTimeout(60000);
        // 4、同时运行中的checkpoint的最大数量
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // 5、最小等待间隔: 上一轮checkpoint结束 到 下一轮checkpoint开始 之间的间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);
        // 6、取消作业时，checkpoint的数据 是否保留在外部系统
        // DELETE_ON_CANCELLATION:主动cancel时，删除存在外部系统的chk-xx目录 （如果是程序突然挂掉，不会删）
        // RETAIN_ON_CANCELLATION:主动cancel时，外部系统的chk-xx目录会保存下来
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 7、允许 checkpoint 连续失败的次数，默认0--》表示checkpoint一失败，job就挂掉
        checkpointConfig.setTolerableCheckpointFailureNumber(10);


        // TODO 设置状态后端
        //如果不设置状态后端,默认是memory在jvm存储,下次回复时这个状态并不能从检查点回复
        //报错了，本地找不到rockdbd的lib库
        EmbeddedRocksDBStateBackend rock = new EmbeddedRocksDBStateBackend();
        rock.setDbStoragePath("hdfs://project1:8020/flink_need/rockdb_state");
        env.setStateBackend(rock);


        //env.setStateBackend( new FsStateBackend("hdfs://project1:8020/flink_cep"));

        //TODO kafka设置检查点提交
        Properties properties = new Properties();
        //将自动提交关闭
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, A1_ConfigProperty.KAFKA_SERVER);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"AAA");
        FlinkKafkaConsumer<String> kafka = new FlinkKafkaConsumer("maxwell", new SimpleStringSchema(), properties);

        kafka.setCommitOffsetsOnCheckpoints(true)  ;
        kafka.assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());

        DataStreamSource<String> kafkaDS = env.addSource(kafka);

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
                String create_time = value.getString("create_time");
                String hours = create_time.substring(0, 13);
                return hours;
            }
        });

        //TODO 目前问题还是valueState重启后不能恢复
        //必须设置状态后端，不能用默认的/
        SingleOutputStreamOperator<Integer> process = keyed.process(new KeyedProcessFunction<String, JSONObject, Integer>() {
            MapState<String, Double> map;
            ValueState<Integer> total;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                MapStateDescriptor descrip = new MapStateDescriptor<>("map", Types.STRING, Types.DOUBLE);
                ValueStateDescriptor<Integer> valueDescrip = new ValueStateDescriptor<>("value", Types.INT);
                map = getRuntimeContext().getMapState(descrip);
                total = getRuntimeContext().getState(valueDescrip);
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<Integer> out) throws Exception {
                String userid = value.getString("userid");
                Double price = value.getDouble("price");
                String orderno = value.getString("orderno");

                if (!map.contains(userid)) {
                    map.put(userid, price);
                    if (total.value() == null) {
                        System.out.println("初始化0");
                        total.update(0);
                    }
                    total.update(total.value() + 1);
                    out.collect(total.value());
                    System.out.println(orderno);

                } else {
                    map.put(userid, map.get(userid) + price);
                    System.out.println(userid + "已经消费过了");
                }

            }
        });

        process.print();
        env.execute();
    }
}
