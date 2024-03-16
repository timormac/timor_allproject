package lpc.utils.sqlanalyse;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @Title: A_CalsiteDemo
 * @Package: lpc.utils.sqlanalyse
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/15 15:31
 * @Version:1.0
 */
public class A_Calsite解析sql {

    public static void main(String[] args) {

        String sql = "SELECT CASE \n" +
                "\t\tWHEN count(1) > 0 THEN 1\n" +
                "\t\tELSE 0\n" +
                "\tEND AS is_audit_record\n" +
                "FROM (\n" +
                "\tSELECT a.khh AS fund_acc_no\n" +
                "\tFROM realtime.\"RT_EGT_CIF_TYWQQ\" A\n" +
                "\tWHERE a.ywdm = '23160'\n" +
                "\t\tAND a.sqrq >= to_char(SYSDATE - 1, 'YYYYmmdd')\n" +
                "\t\tAND a.clzt IN (2, 8, 9, 33, 34, 38, 41, 42, 43, 44, 80)\n" +
                "\t\tAND a.khh = 1\n" +
                "\tUNION ALL\n" +
                "\tSELECT fund_account AS fund_acc_no\n" +
                "\tFROM (\n" +
                "\t\tSELECT operator_type, create_date, request_no, operator_status\n" +
                "\t\tFROM realtime.\"RT_CRHKH_CRH_SBC_AUDITRECORD\"\n" +
                "\t\tWHERE operator_type = '0'\n" +
                "\t\t\tAND operator_status = '-1'\n" +
                "\t\t\tAND create_date >= to_number(to_char(SYSDATE - 1, 'yyyymmdd'))\n" +
                "\t\tUNION\n" +
                "\t\tSELECT operator_type, create_date, request_no, operator_status\n" +
                "\t\tFROM staging.\"CRHKH_CRH_HIS_HIS_AUDITRECORD\"\n" +
                "\t\tWHERE operator_type = '0'\n" +
                "\t\t\tAND operator_status = '-1'\n" +
                "\t\t\tAND create_date >= to_number(to_char(SYSDATE - 1, 'yyyymmdd'))\n" +
                "\t) t, (\n" +
                "\t\tSELECT request_no, businflow_no, fund_account\n" +
                "\t\tFROM realtime.\"RT_CRHKH_CRH_SBC_ACCEPTANCE\"\n" +
                "\t\tWHERE fund_account = 1\n" +
                "\t\t\tAND businflow_no = 600134\n" +
                "\t\tUNION\n" +
                "\t\tSELECT request_no, businflow_no, fund_account\n" +
                "\t\tFROM staging.\"CRHKH_CRH_HIS_HIS_ACCEPTANCE\"\n" +
                "\t\tWHERE fund_account = 1\n" +
                "\t\t\tAND businflow_no = 600134\n" +
                "\t) a\n" +
                "\tWHERE t.request_no = a.request_no\n" +
                ") t";

        //System.out.println(sql);


        Map<String, Set<String>> map = A1_Tools.getTableColume(sql);

        for ( String tb : map.keySet() ) {
            String res = tb + ": " ;
            Set<String> strings = map.get(tb);

            for (String s : strings) {
                res += s +",";
            }
            System.out.println(res);
        }


    }

}
