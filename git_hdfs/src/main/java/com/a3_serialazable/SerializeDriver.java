package com.a3_serialazable;

import com.a2_wordcount.WordCountDriver;
import com.a2_wordcount.WordCountMapper;
import com.a2_wordcount.WordCountReducer;
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
 * @Title: SerializeDriver
 * @Package: com.a3_serialazable
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/17 19:52
 * @Version:1.0
 */
public class SerializeDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
        JobConf jconf = new JobConf(configuration);
        Job job = Job.getInstance(jconf);

        Calendar calendar = Calendar.getInstance();
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        int date = calendar.get(Calendar.DATE);
        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        int minute = calendar.get(Calendar.MINUTE);
        int second = calendar.get(Calendar.SECOND);
        String date_time = ""+year +"-"+month+"-"+date+"-"+hour+"-"+minute+"-"+second  ;

        // 2 设置driver类加载
        job.setJarByClass(SerializeDriver.class);

        // 3 设置map和reduce类
        job.setMapperClass(SerializeMapper.class);
        job.setReducerClass(SerializeReducer.class);

        // 4 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(UserSerializeDao.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(UserSerializeDao.class);

        // 6 设置输入和输出路径 会读取输入路径下所有的文件
        FileInputFormat.setInputPaths( job ,new Path("/Users/timor/Desktop/data/input/serializable"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/timor/Desktop/data/output/sercount"+date_time));

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
