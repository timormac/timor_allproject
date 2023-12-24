package com.lpc.realtime.warehouse.a9_test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * @Author Timor
 * @Date 2023/12/22 16:26
 * @Version 1.0
 */
public class JsonData {
    public static void main(String[] args) {

        String id = "haha" ;
        String s="{\"address\":\"北京市\",\"age\":20,\"name\":\"张三\"" +
                ",\"id\":"+ "\""+id +"\"" +
                "}";
        System.out.println(s);
        JSONObject jsonObject = JSON.parseObject(s);
        System.out.println(jsonObject.getString("id"));

    }
}
