package com.lpc;

import com.alibaba.fastjson.JSONObject;

/**
 * @Author Timor
 * @Date 2024/3/1 12:37
 * @Version 1.0
 */
public class Test3 {
    public static void main(String[] args) {

      //  String value = "2021-12-31 17:35:0.394,,,,,,,,,,";
       // String value = "2021-12-31 17:35:11.394,WFT,8479,dej6r@yahoo.com,上海,69.27,\"{\"\"ismain\"\":0\",level:70,ts:76,ver:48},";
        String value = "2021-12-31 17:35:10.522,734489,WET,5681,4ihgzaw8rvj@gmail.com,广东,155.92,\"{\"\"ismain\"\":1\",level:28,ts:98,ver:54}";

        String[] split = value.split(",\"\\{");

        //过滤这类数据2021-12-31 17:35:11.394,,,,,,,,,,格式数据
        String json ="";
        if(split.length>1){

            String  info1 = split[0];
            System.out.println(info1);
            String[] infoSplit = info1.split(",");
            System.out.println(infoSplit.length);

            //过滤缺少字段数据
            if(infoSplit.length==7){
                //清洗json格式
                String  parameterDetail = split[1];
                parameterDetail = value.split("\\{")[1];
                String r1 = parameterDetail.replace("\"\"ismain\"\":1\"", "\"ismain\":1");
                String r2 = r1.replace("\"\"ismain\"\":0\"", "\"ismain\":0");
                json = "{" + r2;
                Interview.UserLog userLog = new Interview.UserLog( infoSplit[0],infoSplit[1] ,infoSplit[2],infoSplit[3],infoSplit[4],infoSplit[5],infoSplit[6],json);
                System.out.println(userLog);
            }else {
                System.out.println("脏数据"+value);
            }
        }else {
            System.out.println("脏数据"+value);
        }




    }
}
