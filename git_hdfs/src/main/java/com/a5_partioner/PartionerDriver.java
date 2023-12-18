package com.a5_partioner;

import com.a2_wordcount.WordCountMapper;
import com.a2_wordcount.WordCountReducer;
import com.a3_serialazable.SerializeMapper;
import com.a3_serialazable.SerializeReducer;
import com.a3_serialazable.UserSerializeDao;
import com.a4_combinInput.CombineDriver;
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
 * @Title: PartionerDriver
 * @Package: com.a5_partioner
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/18 18:09
 * @Version:1.0
 */
public class PartionerDriver {
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
        job.setJarByClass(PartionerDriver.class);
        //设置分区数量
        job.setNumReduceTasks(2);
        //设置自定义分区器
        job.setPartitionerClass(UdfPartitoner.class);
        //继续用serialize的map和reduce
        job.setMapperClass(SerializeMapper.class);
        job.setReducerClass(SerializeReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(UserSerializeDao.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(UserSerializeDao.class);
        FileInputFormat.setInputPaths( job ,new Path("/Users/timor/Desktop/data/input/serializable"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/timor/Desktop/data/output/partition"+date_time));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);


    }
}
