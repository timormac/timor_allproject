package timor.a1_demo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import timor.utils.A0_Config;


import java.time.Duration;
import java.time.ZoneId;
import java.util.Properties;

/**
 * @Author Timor
 * @Date 2023/12/21 21:23
 * @Version 1.0
 */
public class A1_SinkToHdfs {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
       // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();
        env.setParallelism(2);
        //指定访问hdfs的用户名
        System.setProperty("HADOOP_USER_NAME", "lpc");
        //开启检查点10分钟执行一次
        env.enableCheckpointing(10*60*1000L, CheckpointingMode.EXACTLY_ONCE);
        //设置状态后端类型
        env.setStateBackend( new HashMapStateBackend() );
        //设置检查点存储位置
        env.getCheckpointConfig().setCheckpointStorage(A0_Config.HDFS_PATH+"/flink_need/hdfs_sink_checkpoint");
        //增量状态后端,存入检查点
        env.enableChangelogStateBackend(true);

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,A0_Config.KAFKA_SERVERS);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"sinkToHdfs");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer("flink_optimize", new SimpleStringSchema(), properties);
        //设置检查点提交offset
        consumer.setCommitOffsetsOnCheckpoints(true);
        DataStreamSource<String> ds = env.<String>addSource(consumer);

        SingleOutputStreamOperator<JSONObject> map = ds.map(JSON::parseObject);

        KeyedStream<JSONObject, String> keyed = map.keyBy(x -> x.getString("id"));

        SingleOutputStreamOperator<String> process = keyed.process(new KeyedProcessFunction<String, JSONObject, String>() {

            ValueState<Integer> sum;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                sum = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("sum", Types.INT));
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {

                if(sum.value() == null) {
                    sum.update(1);
                }
                String s = "";
                if (value.getDouble("money") > 500) {
                    s += value.getString("address") + "\t" + value.getString("age") + "\t" + value.getString("name") + "\t" + value.getString("id") + "\t" + value.getString("money");
                    out.collect(s);
                    sum.update(sum.value() + 1);
                }

                if (sum.value() % 100 == 0) {
                    System.out.println(ctx.getCurrentKey() + ":" + sum.value());
                }

            }
        });


        Path path = new Path(A0_Config.HDFS_PATH + "/flink_sink_hdfs");

        FileSink fileSink = FileSink
                .forRowFormat(path, new SimpleStringEncoder())
                .withOutputFileConfig( OutputFileConfig.builder()
                        .withPartPrefix("timor") //前缀
                        .withPartSuffix(".log")
                        .build())
                // 按照目录分桶：如下，就是每个小时一个目录
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH", ZoneId.systemDefault()))
                // 文件滚动策略:  20s 或 1m
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofSeconds(20))
                                .withMaxPartSize(new MemorySize(1024L*1024))
                                .build()
                )
                .build();

        //只执行一个文件
        process.sinkTo(fileSink).setParallelism(1);
        env.execute();


    }


}

