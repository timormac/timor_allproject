package a2_summary.a6_stateCoding;


import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @Title: A3_State
 * @Package: summary.a5_stateCoding
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/23 16:38
 * @Version:1.0
 */
public class A3_BroadState {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.fromCollection(Arrays.asList("a", "b"));
        DataStreamSource<String> mainDS = env.fromCollection(Arrays.asList("a", "b"));


        // TODO 先把流广播，并且传一个map状态进去,只能是map,别的不行
        MapStateDescriptor<String, Integer> broadcastMapState = new MapStateDescriptor<>("broadcast-state", Types.STRING, Types.INT);
        BroadcastStream<String> broadcast = ds.broadcast(broadcastMapState);

        // TODO 数据流 和 广播流 connect
        BroadcastConnectedStream<String, String> broadConnect = mainDS.connect(broadcast);


        // TODO 广播连接流，只有一个process方法
        broadConnect.process(
                new BroadcastProcessFunction<String, String, Object>() {

                    //通过上下文获取广播状态，取出里面的值（只读，不能修改）
                         public void processElement(String value, ReadOnlyContext ctx, Collector<Object> out) throws Exception {
                             ReadOnlyBroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(broadcastMapState);
                             broadcastState.get("a1");
                         }

                         //只有广播流才能修改 广播状态
                         public void processBroadcastElement(String value, Context ctx, Collector<Object> out) throws Exception {
                             BroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(broadcastMapState);
                             broadcastState.put("threshold", Integer.valueOf(value));
                         }}

        );



    }
}
