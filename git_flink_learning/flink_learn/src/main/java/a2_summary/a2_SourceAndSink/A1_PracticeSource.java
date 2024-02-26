package a2_summary.a2_SourceAndSink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @Author Timor
 * @Date 2023/12/21 16:37
 * @Version 1.0
 */
public class A1_PracticeSource {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO  ------------------------------------测试生成流-------------------------------------------
        //集合中获取
        DataStreamSource<String> collection = env.fromCollection(Arrays.asList("a", "b"));
        DataStreamSource<String> socket = env.socketTextStream("localhost", 7088);

        //TODO  -------------------------------------fromSouce-------------------------------------------
        //fromSource第一个参数是Source接口,目前实现的有File,Hive,Kafka,DataGeneratorS。

        //FileSource，也可以指定hdfs
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("input/word.txt")).build();

        //kafkaSource ,通过builder创建,可以配置很多参数,详情见kafkaSource
        KafkaSourceBuilder<Object> builder = KafkaSource.builder().setTopics("aa");
        KafkaSource<Object> build = builder.build();

        //注意fromSource方法。必须传WatermarkStrategy规则 ,然后最后要传source类型
        DataStreamSource<String> fileDS = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file");

        //TODO  -------------------------------------addSouce-------------------------------------------
        //addSource比fromSource有更精密的控制,下面用kakfa的SourceFunction举例，详情看kakaSouce
        FlinkKafkaConsumer kafka = new FlinkKafkaConsumer("aa", new SimpleStringSchema(), new Properties());
        //这里能做更精细的控制，控制kafka的offset提交在checkpoint
        kafka.setCommitOffsetsOnCheckpoints(true);
        DataStreamSource dataStreamSource = env.addSource(kafka);


    }

}
