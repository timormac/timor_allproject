package lpc.utils.gdzq.demo;

import lpc.utils.gdzq.dao.Rt_crhkh_crh_wskh_userqueryextinfo;
import lpc.utils.mysql.dao.Dao;
import lpc.utils.mysql.tools.A1_MysqlTool;

import java.sql.Connection;
import java.util.ArrayList;

/**
 * @Title: A1_像mysql导入数据
 * @Package: lpc.utils.mysql.gdzq.demo
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/16 08:28
 * @Version:1.0
 */
public class A1_向mysql导入数据 {
    public static void main(String[] args) throws Exception {

        String json  ="{\n" +
                "\"USER_ID\": \"3413602\",\n" +
                "\"MOBILE_TEL\": \"17832300656\",\n" +
                "\"BRANCH_NO\": \" \",\n" +
                "\"BUSINESS_FLAG_LAST\": \"22107\",\n" +
                "\"REQUEST_STATUS\": \"0\",\n" +
                "\"SUBMIT_DATETIME\": null,\n" +
                "\"CHANNEL_CODE\": \"10632\",\n" +
                "\"LAST_UPDATE_DATETIME\": \"2024-03-15 10:05:51\",\n" +
                "\"CLIENT_NAME\": \"刘嘉浩\",\n" +
                "\"ID_NO\": \"130421200508230659\",\n" +
                "\"BIRTHDAY\": \"20050823\",\n" +
                "\"REQUEST_NO\": \"2593832\"\n" +
                "}";
        Connection connect = A1_MysqlTool.getPoolConnect();

        ArrayList<Dao>  list = new ArrayList<Dao>();

        Rt_crhkh_crh_wskh_userqueryextinfo mes = new Rt_crhkh_crh_wskh_userqueryextinfo();
        mes.mock( json );
        System.out.println( mes );

        list.add(mes);
        A1_MysqlTool.insertTableDaoList(connect, "rt_crhkh_crh_wskh_userqueryextinfo", Rt_crhkh_crh_wskh_userqueryextinfo.class.getName(), list);
        list.clear();



    }
}
