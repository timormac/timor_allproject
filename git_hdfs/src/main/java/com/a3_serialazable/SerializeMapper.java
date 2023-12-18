package com.a3_serialazable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Title: SerializMapper
 * @Package: com.a3_serialazable
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/17 19:50
 * @Version:1.0
 */

//代码有bug,reducer一直没执行
public class SerializeMapper extends Mapper<LongWritable, Text,Text,UserSerializeDao> {
    @Override
    protected void map(LongWritable key, Text value,
                       Mapper<LongWritable, Text, Text, UserSerializeDao>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        UserSerializeDao dao = new UserSerializeDao(split[0],split[1], Integer.valueOf(split[2]), Double.valueOf(split[3]));
        context.write( new Text( split[0] ),dao );

    }
}
