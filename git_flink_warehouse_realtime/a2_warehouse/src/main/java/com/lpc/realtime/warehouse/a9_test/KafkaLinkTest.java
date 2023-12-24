package com.lpc.realtime.warehouse.a9_test;



/**
 * @Author Timor
 * @Date 2023/12/23 23:57
 * @Version 1.0
 */
public class KafkaLinkTest {

    public static void main(String[] args)  {

        String type = "date";

        switch ( (String) type){
            case "datetime":
                type = "varchar";

                break;
            case "date":
                type = "varchar";
                System.out.println(2);
                break;
        }

        System.out.println( type);

    }
}
