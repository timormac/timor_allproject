package com.a3_serialazable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Title: SerializeReducer
 * @Package: com.a3_serialazable
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/17 19:52
 * @Version:1.0
 */
public class SerializeReducer extends Reducer<Text,UserSerializeDao,Text,Text> {
    HashMap<String,Double> map=new HashMap();

    @Override
    protected void reduce(Text key, Iterable<UserSerializeDao> values, Reducer<Text, UserSerializeDao, Text, Text>.Context context) throws IOException, InterruptedException {

        for (UserSerializeDao value : values) {
            String user = value.user;
            if(map.containsKey(user)) map.put( user,map.get(user) + value.money );
            else map.put(user,value.money);
        }

        for (String user : map.keySet()) {
            String rs = user + ":" + map.get(user);
            context.write(key,new Text(rs)  );
        }
        //打印map地址,看不同key的map是不是不同地址
        System.out.println(map);

//        打印结果:
//        {a=6.0, c=2.0, d=3.0}
//        {a=6.0, b=12.0, c=2.0, d=3.0}
//        也就是说不同的key用到的是一个map,所以我一直不清楚为什么老师代码用Text在外面不会出问题？？

    }
}
