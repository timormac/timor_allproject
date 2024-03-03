package a2_summary.a4_KeyedAndNoKeyedOperater;

import a1_flink_learn.dao.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @Author Timor
 * @Date 2024/2/29 9:59
 * @Version 1.0
 */
public class A4_设置事件时间 {

    public static void main(String[] args) {

        //TODO   设置事件时间也是通过watermark. waterMark不只是设置延迟,

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<String> ds = env.fromElements("1","2");

        SingleOutputStreamOperator<String> map = ds.map(s -> s);


        // 定义Watermark策略,<WaterSensor>forBoundedOutOfOrderness,必须加范型在前面，不然后面调用withTimestampAssigner方法会爆红
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(2)) //  指定watermark生成：乱序的，等待3s
                .withIdleness(Duration.ofSeconds(10))  //空闲等待10s，即当10s内其他分区没有数据更新事件时间是，等10s同步水位线
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        System.out.println("调用了获取时间时间方法:" + element.toString()+"recordTS:"+recordTimestamp);
                        //默认单位是毫秒
                        long eventTime = Long.valueOf(element)+ 1000L;
                        System.out.println(eventTime);
                        return eventTime;
                    }
                });

        //设置watermark策略,只能在SingleOutputStreamOperator上设置水位线策略,wiondow的DS没有assignTimestampsAndWatermarks方法
        //keyStream也可以assignTimeStamp不过返回的是SingleOutputStrea,这样无法调用wiondow方法了，所以只能在map时设置水位线规则
        SingleOutputStreamOperator<String> assginDS = map.assignTimestampsAndWatermarks(watermarkStrategy);


    }
}
