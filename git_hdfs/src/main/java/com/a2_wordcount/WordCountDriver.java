package com.a2_wordcount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
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
public class WordCountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Calendar calendar = Calendar.getInstance();
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        int date = calendar.get(Calendar.DATE);
        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        int minute = calendar.get(Calendar.MINUTE);
        int second = calendar.get(Calendar.SECOND);
        String date_time = ""+year +"-"+month+"-"+date+"-"+hour+"-"+minute+"-"+second  ;

        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
        JobConf jconf = new JobConf(configuration);
        Job job = Job.getInstance(jconf);



                // 2 设置driver类加载
        job.setJarByClass(WordCountDriver.class);

        // 3 设置map和reduce类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径 会读取输入路径下所有的文件
        FileInputFormat.setInputPaths( job ,new Path("/Users/timor/Desktop/data/input/wordcount"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/timor/Desktop/data/output/wordcount"+date_time));

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

    }

}

