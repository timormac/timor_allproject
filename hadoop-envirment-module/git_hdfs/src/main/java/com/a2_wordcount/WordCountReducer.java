package com.a2_wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Title: WordCountReducer
 * @Package: com.wordcount
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/17 10:45
 * @Version:1.0
 */

//Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
public class WordCountReducer  extends Reducer<Text, IntWritable, Text,IntWritable> {



    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        System.out.println("调用方啊了");
        int sum = 0;
        for( IntWritable  v:values ) sum += v.get();
        context.write( key,  new IntWritable( sum )   );

    }

}
