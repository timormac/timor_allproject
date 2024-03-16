package lpc.utils.sqlanalyse;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @Title: A1_Tools
 * @Package: lpc.utils.sqlanalyse
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/15 16:05
 * @Version:1.0
 */
public class A1_Tools {

    //获取sql中用到的表，以及表用到的字段
    static Map<String, Set<String> > getTableColume(String sql){


        /**
         * sql处理;,处理${}
         * 并且map输出 有临时表a,t等,并且还有个空
         */


        SqlParser.Config parserConfig = SqlParser.configBuilder()
                .setLex(Lex.ORACLE) // 设置Oracle的引用标识符的词法处理
                .setConformance(SqlConformanceEnum.ORACLE_10) // 设置遵循Oracle 12c及以上版本的SQL方言
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
