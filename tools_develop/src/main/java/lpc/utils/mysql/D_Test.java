package lpc.utils.mysql;

import lpc.utils.mysql.dao.Mock_Cupon;
import lpc.utils.mysql.tools.A1_MysqlTool;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Random;

/**
 * @Author Timor
 * @Date 2024/2/24 20:34
 * @Version 1.0
 */
public class D_Test {
    public static void main(String[] args) throws Exception {

        long stamp = System.currentTimeMillis();
        String format = DateFormatUtils.format(17832300656L, "yyyy-MM-dd HH:mm:ss");
        System.out.println(format);

    }
}
