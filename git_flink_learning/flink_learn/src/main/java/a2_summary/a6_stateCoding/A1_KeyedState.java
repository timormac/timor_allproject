package a2_summary.a6_stateCoding;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @Title: KeyedState
 * @Package: summary.a5_stateCoding
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/23 16:37
 * @Version:1.0
 */
public class A1_KeyedState {
    public static void main(String[] args) {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.fromCollection(Arrays.asList("a", "b"));

        KeyedStream<String, String> key = ds.keyBy(s -> s);

        key.map(new RichMapFunction<String, Object>() {
            ValueState<Integer> value;
            MapState<Integer, Integer> map;
            ListState<Integer> list;
            ReducingState<String> reduce;

            AggregatingState<Integer, Double> agg;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                //TODO Value状态，初始值默认是null
                ValueStateDescriptor<Integer> valueDesc = new ValueStateDescriptor<>("lastVcState", Types.INT);
                value = getRuntimeContext().getState(valueDesc);
                value.value();
                value.update(null);
                value.clear();

                //TODO map状态
                MapStateDescriptor<Integer, Integer> mapDesc = new MapStateDescriptor<>("vcCountMapState", Types.INT, Types.INT);
                map = getRuntimeContext().getMapState(mapDesc);

                //TODO list状态
                ListStateDescriptor<Integer> listDesc = new ListStateDescriptor<>("vcListState", Types.INT);
                list = getRuntimeContext().getListState(listDesc);

                //TODO reduce状态
                ReducingStateDescriptor<String> reduceDesc = new ReducingStateDescriptor<>(
                        "vcSumReducingState",
                        (String s1,String s2)->""+s1.charAt(1)+s2.charAt(1),
                        Types.STRING);
                reduce= getRuntimeContext().getReducingState(reduceDesc);

                reduce.add("abc"); //放入后直接按reduce传的匿名函数操作,更新操作后的值
                reduce.get();
                reduce.clear();

                //TODO aggregate状态,注意agg泛型，是中间状态的泛型，不是结果泛型
                AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double> aggDesc = new AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double>(
                        "vcAvgAggregatingState",
                        new UdfAgg(),
                        Types.TUPLE(Types.INT, Types.INT)
                        );

                agg = getRuntimeContext().getAggregatingState(aggDesc);
                agg.add(null);
                agg.get();
            }

            @Override
            public Object map(String value) throws Exception {
                return null;
            }
        });

    }



    //TODO 功能求平均数 ，泛型<In,Middle,Result>
   static class UdfAgg implements AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>{

        //初始的第一个Middle如何定义的
        public Tuple2<Integer, Integer> createAccumulator() {
            return Tuple2.of(0, 0);
        }

        //一个数据和初始middle处理，更新middle，后续再和中间middle处理
        public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
            accumulator.f0 =  accumulator.f0+1;
            accumulator.f1 = accumulator.f1 +value;
            return accumulator;
        }

        @Override
        public Double getResult(Tuple2<Integer, Integer> accumulator) {

            return (double)(accumulator.f1/accumulator.f0);
        }

        @Override
        public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
            return null;
        }
    }

}

