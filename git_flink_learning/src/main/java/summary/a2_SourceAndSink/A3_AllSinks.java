package summary.a2_SourceAndSink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;


/**
 * @Author Timor
 * @Date 2023/12/21 20:38
 * @Version 1.0
 */
public class A3_AllSinks {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> ds = env.fromElements("a", "b");

        //TODO  -------------------------------------fileSink------------------------------------------

        //如果是hdfs还要设置签名

        FileSink fileSink = FileSink
                .forRowFormat(new Path( "hdfs://project1:8020/aaa"), new SimpleStringEncoder())
                 .withOutputFileConfig( OutputFileConfig.builder()
                        .withPartPrefix("timor") //前缀
                        .withPartSuffix(".log")
                        .build())
                // 按照目录分桶：如下，就是每个小时一个目录
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH", ZoneId.systemDefault()))
                // 文件滚动策略:  1分钟 或 1m
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofSeconds(10))
                                .withMaxPartSize(new MemorySize(1024L*1024))
                                .build()
                )
                .build();

        //TODO  -------------------------------------kafkaSink-------------------------------------------


        ds.sinkTo(fileSink);

        //ds.addSink(   );



    }


}
