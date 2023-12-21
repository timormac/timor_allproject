package summary.a4_operatorInterface;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Arrays;

/**
 * @Author Timor
 * @Date 2023/12/21 16:19
 * @Version 1.0
 */
public class A1_RichFunciton {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds1 = env.fromCollection(Arrays.asList("a", "b"));
        ds1.addSink(
                new RichSinkFunction() {
            @Override
            public void setRuntimeContext(RuntimeContext t) {
                super.setRuntimeContext(t);
            }

            @Override
            public RuntimeContext getRuntimeContext() {
                return super.getRuntimeContext();
            }

            @Override
            public IterationRuntimeContext getIterationRuntimeContext() {
                return super.getIterationRuntimeContext();
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public void invoke(Object value, Context context) throws Exception {

            }

            @Override
            public void writeWatermark(Watermark watermark) throws Exception {

            }

            @Override
            public void finish() throws Exception {

            }
        });

    }


}
