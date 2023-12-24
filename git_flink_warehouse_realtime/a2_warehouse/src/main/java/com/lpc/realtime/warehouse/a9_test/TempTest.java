package com.lpc.realtime.warehouse.a9_test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lpc.realtime.warehouse.a2_utils.Tm_PhoenixUtils;
import com.lpc.realtime.warehouse.a5_functions.dim.A2_ConfigFilterTable;
import com.lpc.realtime.warehouse.a7_app.dim.A2_DimApplication;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @Author Timor
 * @Date 2023/12/22 16:47
 * @Version 1.0
 */
public class TempTest {

    public static void main(String[] args) {

        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        map.put("1","a");
        map.put("2","b");
        map.put("3","c");
        map.put("4","d");

        System.out.println(map);
        String columes = "1,3";

        String str = JSON.toJSONString(map);
        System.out.println(str);

        JSONObject json = JSON.parseObject(str);

        String[] split = columes.split(",");


    }
}
