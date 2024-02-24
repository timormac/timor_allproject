package a2_summary.a1_envAndStream;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;


import java.util.Arrays;

/**
 * @Title: A3_SeriAndDeserType
 * @Package: summary.a1_envAndStream
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/24 11:12
 * @Version:1.0
 */
public class A3_SeriAndDeserType {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds1 = env.fromCollection(Arrays.asList("a", "b"));

        //TODO flink支持的类型
        /**
         * flink流支持的数据类型
         * 基本类型 : String,Integer等
         * collections :List,Map,Set,Enums
         * Tuple封装类
         * Pojos
         */

        //TODO 提供封装工具Types类
        /**
         * flink支持的可序列化接口  TypeInformation和TypeSerializer
         * TypeInformation的实现类有BasicTypeInfo , PojoTypeInfo等
         *Types类提供了很多BasicTypeInfo的基本类型
         */

        //基本类型
        OutputTag<String> output1 = new OutputTag<>("s1",Types.STRING);
        //POJO类型
        OutputTag<A4_POJO> output2 = new OutputTag<>("s1",Types.POJO(A4_POJO.class));



    }
}
