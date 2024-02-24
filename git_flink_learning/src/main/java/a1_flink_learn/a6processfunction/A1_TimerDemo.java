package a1_flink_learn.a6processfunction;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Title: A1_TimerDemo
 * @Package: com.timor.flink.learning.a6processfunction
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/19 20:17
 * @Version:1.0
 */
public class A1_TimerDemo {

    public static void main(String[] args) {

        //要求10s内要是用户登陆超过20次，认为是爬虫
        //10s内超过来了5条数据，认为数据过多，报警

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.socketTextStream("localhost", 7777);


        SingleOutputStreamOperator<Tuple2<String, Long>> flatMap = ds.flatMap((value, out) -> {
            String[] split = value.split(" ");
            try {
                Tuple2<String, Long> tuple = new Tuple2<>(split[0],Long.valueOf(split[1]) );
                out.collect(tuple);

            } catch (Exception e) {
                System.out.println(e);
            }
        });

        WatermarkStrategy<Tuple2<String, Long>> strategy = WatermarkStrategy
                .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                        return element.f1*1000L;
                    }
                });

        flatMap.assignTimestampsAndWatermarks(strategy);

        KeyedStream<Tuple2<String, Long>, Long> keyed = flatMap.keyBy(s -> s.f1);
        //keyed.window( ProcessWindowFunction )


        //如果不keyby那么所有数据共用一个list
        //我想每个时间单独享有list实现不了
        flatMap.process(new ProcessFunction<Tuple2<String, Long>, Object>() {


            @Override
            public void processElement(Tuple2<String, Long> value, ProcessFunction<Tuple2<String, Long>, Object>.Context ctx,
                                       Collector<Object> out) throws Exception {

                TimerService timerService = ctx.timerService();
                //当前事件事件
                Long timestamp = ctx.timestamp();
                //注册10秒后的定时器，这个方法直接写多少秒后就行，不需要加本身时间
                timerService.registerEventTimeTimer( 10*1000L );



            }

            @Override
            public void onTimer(long timestamp, ProcessFunction<Tuple2<String, Long>, Object>.OnTimerContext ctx,
                                Collector<Object> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
            }



        });

    }
}
