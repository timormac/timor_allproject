package a2_summary.a5_winodwStream;

import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @Author Timor
 * @Date 2024/2/26 8:50
 * @Version 1.0
 */
public class A0_window的基本操作 {

    public static void main(String[] args) {


        /**
         *  触发器、移除器： 现成的几个窗口，都有默认的实现，一般不需要自定义
         *
         *  以 时间类型的 滚动窗口 为例，分析原理：
         TODO 1、窗口什么时候触发 输出？
         时间进展 >= 窗口的最大时间戳（end - 1ms）

         TODO 2、窗口是怎么划分的？
         start= 向下取整，取窗口长度的整数倍
         end = start + 窗口长度

         窗口左闭右开 ==》 属于本窗口的 最大时间戳 = end - 1ms

         TODO 3、窗口的生命周期？
         创建： 属于本窗口的第一条数据来的时候，现new的，放入一个singleton单例的集合中
         销毁（关窗）： 时间进展 >=  窗口的最大时间戳（end - 1ms） + 允许迟到的时间（默认0）

         */


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.fromElements("a", "b");

        KeyedStream<String, String> keyBy = ds.keyBy(s -> s);

        //TODO 可调用的方法
        WindowedStream<String, String, TimeWindow> ws = keyBy.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sideOutputLateData(null)  //迟到数据放入测输出流
                .allowedLateness(Time.seconds(2))  //窗口延迟2秒关闭
        ;


        //TODO process函数是窗口关闭时处理迭代器
        ws.process(null);
        ws.apply(null); //也是处理迭代器，不过功能没process多，弃用
        //TODO reduce和aggregate是来一条处理一条,最后返回累加结果
        ws.reduce(null);
        ws.aggregate(null);


        //TODO aggregate( aggregte,process ),这个是累加器执行,最后把一个结果传给process,结合2个有点,process的迭代器只有一个数据
        //ws.aggregate(null,null)


        /**
         * 计数窗口2种
         * slide滑动
         * tumbling滚动
         */
       //全局计数窗口
        AllWindowedStream<String, GlobalWindow> win = ds.countWindowAll(1000);
        //全局计时窗口
        AllWindowedStream<String, TimeWindow> win3 = ds.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)));


        //keyby计数窗口
        WindowedStream<String, String, GlobalWindow> keyCOUNT = keyBy.countWindow(10);


        /**
         * 计时窗口7种实现类
         * slide滑动窗口，2种:事件时间，处理时间
         * tumbling滚动窗口，2种:事件时间，处理时间
         * session会话窗口，2种:事件时间，处理时间
         * global全局窗口，只有一个类
         */

        //滚动 处理时间
        TumblingProcessingTimeWindows tmProcess = TumblingProcessingTimeWindows.of( Time.seconds(1000L) );
        //滚动 事件时间
        TumblingEventTimeWindows tmEvent = TumblingEventTimeWindows.of(Time.seconds(1000L));
        //滑动 处理时间
        SlidingProcessingTimeWindows slidPro = SlidingProcessingTimeWindows.of(Time.seconds(100), Time.seconds(10));
        //滑动 事件时间
        SlidingEventTimeWindows slidEv = SlidingEventTimeWindows.of(Time.seconds(100), Time.seconds(10));
        //会话 事件时间
        EventTimeSessionWindows sessionEv = EventTimeSessionWindows.withGap(Time.seconds(100));
        //会话 处理时间
        ProcessingTimeSessionWindows sessionPro = ProcessingTimeSessionWindows.withGap(Time.seconds(100));
        //全局窗口,需要自己定义触发器，不然没意义
        GlobalWindows globalWindows = GlobalWindows.create();


    }
}
