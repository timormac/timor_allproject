package com.a1_hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Title: A1_HdfsMakeDir
 * @Package: com.lpc
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/14 18:23
 * @Version:1.0
 */

    //主要是获取一个连接FileSystem，其中Configuration和URI都是需要的参数
    //URI和Configuration有很多包都有，注意别选错了，点进FileSystem去看需要的是哪个包类
public class A1_HdfsMakeDir {

    @Test
    public void mkdirs() throws URISyntaxException, IOException {


        Configuration configuration = new Configuration();

        URI uri = new URI("hdfs://project1:8020");

        FileSystem fs = FileSystem.get(uri, configuration);

        fs.mkdirs( new Path("/java-code-mkdir") );

        fs.close();


    }


}


