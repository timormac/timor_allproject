package lpc.utils.flinksql;

/**
 * @Title: A1_FlinksqlTools
 * @Package: lpc.utils.flinksql
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/16 09:15
 * @Version:1.0
 */
public class A1_FlinksqlTools {


    /** 从maxwell获取kafka流后,根据mysql的表名,生成临时视图,减少复制粘贴,目标如下
     "select " +
     "data['user_id'] as user_id , " +
     " from kafka_maxwell" +
     " where `table` = 'rt_crhkh_crh_wskh_userqueryextinfo' ");
     */
    public  static void flinktableDDL(String str){

    }

}
