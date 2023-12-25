package com.a4_combinInput;


import com.a2_wordcount.WordCountMapper;
import com.a2_wordcount.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Calendar;


/**
 * @Title: WordCountDriver
 * @Package: com.wordcount
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/17 10:45
 * @Version:1.0
 */
public class CombineDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Calendar calendar = Calendar.getInstance();
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        int date = calendar.get(Calendar.DATE);
        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        int minute = calendar.get(Calendar.MINUTE);
        int second = calendar.get(Calendar.SECOND);
        String date_time = ""+year +"-"+month+"-"+date+"-"+hour+"-"+minute+"-"+second  ;

        Configuration configuration = new Configuration();
        JobConf jconf = new JobConf(configuration);
        Job job = Job.getInstance(jconf);
        //设置输入类为CombineText，默认的是TextInputFormat
        job.setInputFormatClass(CombineTextInputFormat.class);
        //设置虚拟切片值13421772，单位是 1024byts*1024kb*128M
        CombineTextInputFormat.setMaxInputSplitSize(job,134217728);
        job.setJarByClass(CombineDriver.class);
        //继续用wordcount的map和reduce
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths( job ,new Path("/Users/timor/Desktop/data/input/combine"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/timor/Desktop/data/output/combine"+date_time));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }

}

