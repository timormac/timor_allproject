package a2_summary.a8_sql.tools;


import lpc.utils.mysql.tools.A1_ConfigProperty;

/**
 * @Author Timor
 * @Date 2024/2/28 16:26
 * @Version 1.0
 */
public class A0_SQLConnect {

//    public static String kafkaSourceDDL(String topic, String groupid, KafkaOffsetEnum mode, KafkaFormatEnum format){
//        String sql =" WITH (\n" +
//                "  'connector' = 'kafka',\n" +
//                        "  'properties.bootstrap.servers'= '" + A1_ConfigProperty.KAFKA_SERVER +"',\n"+
//                        "  'topic' = '" +  topic  +"',\n" +
//                        "  'properties.group.id' = '" + groupid  +"',\n" +
//                        "  'scan.startup.mode' = '"+  mode.getFormat() +"',\n" +
//                        "  'format' = '" + format.getValue()  +"'\n" +
//                        ") \n";
//        return sql;
//
//    }
//
//
//    public  static String kafkaSourceDDL(String topic,String groupid){
//        return  kafkaSourceDDL(topic,groupid, KafkaOffsetEnum.LATEST,KafkaFormatEnum.JSON);
//    }



}
