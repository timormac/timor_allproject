package com.a2_wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Title: WordCountMapper
 * @Package: com.wordcount
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/17 10:44
 * @Version:1.0
 */
//Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
public class WordCountMapper  extends Mapper<LongWritable,Text,Text,IntWritable> {

    IntWritable   v = new IntWritable(1);
    //默认读取文件是按一行一行获取的数据,map方法的key是传入的改行在文件中的下标,value是改行数据
    //contex.write必须传一个,k-v2个参数，如果我只要一个k呢，不清楚为什么这么设计
    @Override
    //map(KEYIN key, VALUEIN value,Context context)
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        System.out.println("调用map方法了");
        String line = value.toString();
        String[] lines = line.split(" ");
        for( String word:lines ) {
            System.out.println(word.toString());
            context.write(  new Text(word) ,v );
        }
    }

    @Override
    public void run(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        System.out.println("run方法");
        super.run(context);
    }
}
