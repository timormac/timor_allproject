package com.a5_partioner;

import com.a3_serialazable.UserSerializeDao;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Title: UdfPartitoner
 * @Package: com.a5_partioner
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/18 18:11
 * @Version:1.0
 */
public class UdfPartitoner extends Partitioner<Text, UserSerializeDao> {
    @Override
    public int getPartition(Text text, UserSerializeDao userSerializeDao, int numPartitions) {
        return text.hashCode()%2;
    }
}
