package com.timor.flink.learning;


import com.timor.flink.learning.a1wordcountdemo.A1_WordCountBatch;
import com.timor.flink.learning.dao.WaterSensor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import com.timor.flink.learning.dao.WaterSensor;
/**
 * @Title: Test
 * @Package: com.timor.flink.learning
 * @Description:
 * @Author: XXX
 * @Date: 2023/6/1 18:40
 * @Version:1.0
 */
public class Test {
    public static void main(String[] args) throws Exception {

        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> lineDS = env.readTextFile("input_data/wordcount.txt");

        //flatMap方法需要传一个对象，是实现了FlatMapFunction接口的对象
        FlatMapOperator<String, Object> flatmap = lineDS.flatMap((value, out) -> {
            String[] splits = value.split(" ");
            for (String split : splits) out.collect(split);
        });


        env.execute();

    }

}

