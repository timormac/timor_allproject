package com.lpc.realtime.warehouse.a2_utils;

import com.alibaba.fastjson.JSON;

/**
 * @Author Timor
 * @Date 2023/11/2 18:44
 * @Version 1.0
 */
public class Tm_JsonTool {

    public static  boolean isJSON( String str ){

        try{
            JSON.parseObject(str);
            return true;
        } catch ( Exception e ){
            return  false;
        }
    }
}
