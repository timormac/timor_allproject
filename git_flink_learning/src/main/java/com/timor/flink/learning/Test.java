package com.timor.flink.learning;


import com.timor.flink.learning.dao.WaterSensor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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

        long l = System.currentTimeMillis();
        System.out.println(l);

        System.out.println( "9".equals(9)  );

    }

}

