package d_面向对象编程;

import java.util.LinkedHashMap;

/**
 * @Title: A2_代码块执行顺序
 * @Package: e_objectprogramming
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/23 08:34
 * @Version:1.0
 */
public class A2_代码块执行顺序 {


    //TODO 代码块会在构造器之前调用，即使放在构造器下面
    //TODO 变量初始化会在构造器之前,因此propertyMap如果不先赋值,propertyMap.put()会报空指针
    //TODO 代码块执行赋值后,构造器会把propertyMap重置
    public LinkedHashMap<String ,String> propertyMap = new LinkedHashMap();
    {
        propertyMap.put("connector","kafka");
    }

    public A2_代码块执行顺序(LinkedHashMap<String, String> property) {
        this.propertyMap = property;
    }



}
