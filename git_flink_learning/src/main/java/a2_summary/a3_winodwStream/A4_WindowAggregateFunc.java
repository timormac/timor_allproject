package a2_summary.a3_winodwStream;

import org.apache.arrow.flatbuf.Decimal;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import a2_summary.a1_envAndStream.A4_POJO;

/**
 * @Title: A4_WindowAggregateFunc
 * @Package: summary.a3_winodwStream
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/24 20:17
 * @Version:1.0
 */
public class A4_WindowAggregateFunc {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.fromElements("a", "b");
        //滑动事件窗口,步长100
        AllWindowedStream<String, GlobalWindow> win2 = ds.countWindowAll(1000, 100);

        /**
         * keyed流的Aggregate()有很多参数方式
         * 1.放Aggregate
         * 2.Aggregate + ProcessFunction
         * 3.Aggreagte + WindouwFunction
         */


        /**
         *AggregateAndProcess和Agg与process的区别
         *process会把所有数据先缓存，当窗口结束时统一计算，并且能拿到context
         *aggregate来一条算一条，是按key聚合最后每种ke，只返回一条数据
         *2者结合,先执行aggregate，来一条算一条，然后agg将result方法的结果传递给process的iterator
         * 所以process中的Iterable<String> elements只有一条数据,然后通过out.collect来输出
         */
        SingleOutputStreamOperator<Decimal> aggregate = win2.aggregate(new TmpAgg(), new TmpProcess());

    }
}

class TmpAgg implements AggregateFunction<String, A4_POJO,Integer>{
    @Override
    public A4_POJO createAccumulator() {
        return null;
    }
    @Override
    public A4_POJO add(String value, A4_POJO accumulator) {
        return null;
    }
    @Override
    public Integer getResult(A4_POJO accumulator) {
        return null;
    }
    @Override
    public A4_POJO merge(A4_POJO a, A4_POJO b) {
        return null;
    }
}

class TmpProcess extends ProcessAllWindowFunction<Integer, Decimal, GlobalWindow> {
    @Override
    public void process(ProcessAllWindowFunction<Integer, Decimal, GlobalWindow>.Context context, Iterable<Integer> elements, Collector<Decimal> out) throws Exception {

    }
}
