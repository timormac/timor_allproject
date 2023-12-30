package summary.a3_StreamType;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @Title: A7_JoinStream
 * @Package: summary.a3_StreamType
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/24 12:25
 * @Version:1.0
 */
public class A7_JoinStream {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds1 = env.fromCollection(Arrays.asList("a", "b"));
        DataStream<String> ds2 = env.fromCollection(Arrays.asList("a", "b"));
        /**
         * JoinedStreams,是一个新。内部有2个流属性
         * 只有一个where方法
         * 好像没j8用啊！
         */
        JoinedStreams<String, String> join2 = ds1.join(ds2);

        //只有一个where方法
        JoinedStreams<String, String>.Where<Object> where = join2.where(new KeySelector<String, Object>() {
            @Override
            public Object getKey(String value) throws Exception {
                return null;
            }
        });


    }
}
