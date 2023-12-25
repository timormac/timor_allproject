package summary.a3_winodwStream;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.jws.Oneway;

/**
 * @Title: A3_WindowProcess
 * @Package: summary.a3_winodwStream
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/24 19:18
 * @Version:1.0
 */
public class A3_WindowProcessFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.fromElements("a", "b");

        //滑动事件窗口,步长100
        AllWindowedStream<String, GlobalWindow> win2 = ds.countWindowAll(1000, 100);


        //时间窗口的process
        AllWindowedStream<String, TimeWindow> time = ds.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1000L)));

        SingleOutputStreamOperator<Integer> process = time.process(
                new ProcessAllWindowFunction<String, Integer, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<String, Integer, TimeWindow>.Context context,
                                Iterable<String> elements, Collector<Integer> out) throws Exception {

            }
        });



        OutputTag<String> outputTag = new OutputTag<>("late", Types.STRING);
        //获取窗口自动提交的迟到数据
        SideOutputDataStream<String> sideOutput = process.getSideOutput(outputTag);


    }
}
