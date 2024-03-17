package lpc.utils.gdzq.sqlanalyse;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import java.io.IOException;
import java.util.*;

/**
 * @Title: CalsitTool
 * @Package: lpc.utils.gdzq.sqlanalyse
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/17 11:05
 * @Version:1.0
 */
public class CalciteTool {
    public static void main(String[] args) throws IOException {

        String path = "/Users/timor/Desktop/未命名文件夹/元数据变更风险评估.csv";
        String colName = "custom_sql";
        ArrayList<String> columeList = CSVTool.getColumeList(path, colName);

        String str = columeList.get(0);

        //替换掉${begin}等参数
        String pattern = "\\$\\{[^}]*\\}";
        String newSQL = str.replaceAll(pattern, "1");

        //替换掉;号
        String resultSQL = newSQL.replaceAll(";", "");

        resultSQL = "SELECT a.name, b.salary FROM employee a INNER JOIN department b ON a.dep_id = b.id WHERE a.age > 30";

        //System.out.println(resultSQL);

        Map<String, Set<String>> map = getTableColumeTest(resultSQL);

        for (String key : map.keySet()) {

            String res = key + ":";

            for (String col : map.get(key)) {
                res+= col + ", ";
            }
            System.out.println( res );
        }

        //TODO 目前的问题是把库识别成了表,然后表都是用的别名，识别不出来原名字


        /**
         * :P1, SUBMIT_DATE_TIME, STATUS, T, OP_BRANCH_NAME,
         * P1:BRANCH_NO, BRANCH_NAME,
         * REALTIME:RT_CRHKH_CRH_SBC_ACCEPTANCE,
         * T:OP_BRANCH_NO, SUBMIT_DATE_TIME, STATUS, FUND_ACCOUNT, BUSINFLOW_NO, AUDIT_ROLE_IDS,
         * STAGING:CRHKH_CRH_USER_ALLBRANCH, CRHKH_CRH_HIS_HIS_ACCEPTANCE,
         */


    }


    static Map<String, Set<String>> getTableColumeTest(String sql){

        SqlParser.Config parserConfig = SqlParser.configBuilder()
                .setLex(Lex.ORACLE) // 设置为Oracle语法
                .setConformance(SqlConformanceEnum.ORACLE_10) // 设置遵循Oracle版本
                .build();

        SqlParser parser = SqlParser.create(sql, parserConfig);

        TableFieldExtractor extractor = null;

        try {
            // Parse the SQL statement
            SqlNode parsedSql = parser.parseQuery();

            // Create a visitor to extract table names and fields
            extractor = new TableFieldExtractor();
            parsedSql.accept(extractor);

            // Print the results


        } catch (SqlParseException e) {
            System.err.println("Error parsing SQL: " + e.getMessage());
        }

        return extractor.getTableFields();

    }


    private static class TableFieldExtractor extends SqlShuttle {
        private Map<String, Set<String> > tableFields = new HashMap<>();

        public Map<String, Set<String>> getTableFields() {
            return tableFields;
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            if (id.isSimple()) {
                // If the identifier is simple, it's not prefixed with a table name
                // You might want to handle this differently depending on your use case
                tableFields.computeIfAbsent("", k -> new HashSet<>()).add(id.getSimple());
            } else {
                String tableName = id.names.get(0);
                String fieldName = id.names.get(1);
                tableFields.computeIfAbsent(tableName, k -> new HashSet<>()).add(fieldName);
            }
            return super.visit(id);
        }
    }



}
