package a2_kafka_test.a1_user_create_interrupt;

import com.alibaba.fastjson.JSON;

/**
 * @Title: Test
 * @Package: PACKAGE_NAME
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/18 07:14
 * @Version:1.0
 */
public class Test {

    public static void main(String[] args) {


        DataTemplate dataTemplate = new DataTemplate("17832300656","12345", 2);

        String jsonData = dataTemplate.jsonData;

        String string = JSON.parseObject(jsonData)
                .getJSONObject("message")
                .getJSONObject("data")
                .getString("LAST_UPDATE_DATETIME");

        System.out.println(string);
        System.out.println(dataTemplate.jsonData);


    }
}
