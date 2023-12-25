package com.a1_hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Author Timor
 * @Date 2023/10/14 20:35
 * @Version 1.0
 */
public class A2_HdfsClientOperate {

    FileSystem  fs;

    @Before
    public  void init() throws URISyntaxException, IOException {
        Configuration configuration = new Configuration();
        URI uri = new URI("hdfs://project1:8020");
        fs = FileSystem.get(uri, configuration);

    }

    @Test
    public void mkdirs() throws URISyntaxException, IOException {
        fs.mkdirs(  new Path("/java-code-mkdir/aa") );
        //上传文件
        fs.copyFromLocalFile(false,new Path("./a.txt"),new Path("/java-code-mkdir/"));
        //下载文件
        fs.copyToLocalFile(new Path("/"),new Path("./"));
    }

    @After
    public void close() throws IOException {
        fs.close();
    }

}
