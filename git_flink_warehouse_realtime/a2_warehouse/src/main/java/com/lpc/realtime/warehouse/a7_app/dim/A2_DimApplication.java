package com.lpc.realtime.warehouse.a7_app.dim;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.lpc.realtime.warehouse.a2_utils.A1_ConfigProperty;
import com.lpc.realtime.warehouse.a2_utils.Tm_KafkaUtils;
import com.lpc.realtime.warehouse.a2_utils.Tm_PhoenixConnectPool;
import com.lpc.realtime.warehouse.a2_utils.Tm_PhoenixUtils;
import com.lpc.realtime.warehouse.a5_functions.dim.A1_FilterConfig;
import com.lpc.realtime.warehouse.a5_functions.dim.A2_ConfigFilterTable;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;

import java.util.Map;

/**
 * @Author Timor
 * @Date 2023/12/22 21:03
 * @Version 1.0
 */
public class A2_DimApplication {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        env.setStateBackend(new HashMapStateBackend());

        env.enableCheckpointing(1*60*1000L, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointStorage(A1_ConfigProperty.HDFS_PATH+"/flink_need/checkpoint_dir/A2_DimApplication");

        System.setProperty("HADOOP_USER_NAME","lpc");

        FlinkKafkaConsumer<String> kafkaConsumer = Tm_KafkaUtils.getKafkaConsumer("maxwell", "A2_DimApplication");

        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);

        DataStreamSource<String> ds = env.addSource(kafkaConsumer);

        OutputTag<String> wrongData = new OutputTag<String>("wrongData", Types.STRING );

        OutputTag<String> config = new OutputTag<String>("config", Types.STRING );

        SingleOutputStreamOperator<JSONObject> filter = ds.process( new A1_FilterConfig(wrongData,config) );

        SideOutputDataStream<String> configStream = filter.getSideOutput(config);

        //广播状态
        MapStateDescriptor<String, String> broadstate = new MapStateDescriptor<>("broadstate", Types.STRING, Types.STRING);

        //创建带状态的广播流
        BroadcastStream<String> broadcast = configStream.broadcast(broadstate);

        SideOutputDataStream wrongJsonStream = filter.getSideOutput(wrongData);

        BroadcastConnectedStream<JSONObject, String> connect = filter.connect(broadcast);

        SingleOutputStreamOperator<JSONObject> configedTableStream = connect.process(new A2_ConfigFilterTable(broadstate));

        configedTableStream.addSink(new RichSinkFunction<JSONObject>() {
            DruidDataSource druidDataSource;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                druidDataSource = Tm_PhoenixConnectPool.getDruidDataSource();
            }
            @Override
            public void invoke(JSONObject value, Context context) throws Exception {
                DruidPooledConnection connection = druidDataSource.getConnection();
                String sinktable = value.getString("sinktable");
                Map<String, Object> innerMap = value.getInnerMap();
                innerMap.remove("sinktable");
                Tm_PhoenixUtils.insertHbaseTable(connection,"warehouse",sinktable,innerMap);
            }
        });

        wrongJsonStream.print();

        env.execute();

    }


}
