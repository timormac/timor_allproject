package com.lpc.datamock.tools;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @Title: A0_PropertyUtils
 * @Package: tools
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/20 16:13
 * @Version:1.0
 */
public class A0_PropertyUtils {
    private String name;
    private Properties   property;

    public A0_PropertyUtils(String fileName) throws IOException {
        this.name=fileName;
        this.setProperty();
    }

    //要改成public不然别的项目不能用，创建玩对象后
    public Properties getProperty(){
        return this.property;
    }

    private void setProperty() throws IOException {

        InputStream resource= this.getClass().getClassLoader().getResourceAsStream(name);
        Properties properties = new Properties();
        properties.load(resource);
        this.property =properties;

    }
}
