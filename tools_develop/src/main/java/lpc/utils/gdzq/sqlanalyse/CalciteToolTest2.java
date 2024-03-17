package lpc.utils.gdzq.sqlanalyse;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @Title: CalciteToolTest2
 * @Package: lpc.utils.gdzq.sqlanalyse
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/17 14:28
 * @Version:1.0
 */
public class CalciteToolTest2 {

    public static void main(String[] args) {
        String sql = "SELECT a.name, b.salary FROM employee a INNER JOIN department b ON a.dep_id = b.id WHERE a.age > 30";

        System.out.println(getTableColumeTest(sql));

    }




    static Map<String, Set<String>> getTableColumeTest(String sql) {
        SqlParser.Config parserConfig = SqlParser.configBuilder()
                .setLex(Lex.ORACLE) // 设置为Oracle语法
                .setConformance(SqlConformanceEnum.ORACLE_10) // 设置遵循Oracle版本
                .build();

        SqlParser parser = SqlParser.create(sql, parserConfig);

        TableFieldExtractor extractor = new TableFieldExtractor();

        try {
            // Parse the SQL statement
            SqlNode parsedSql = parser.parseQuery();

            // Create a visitor to extract table names and fields
            // extractor = new TableFieldExtractor(); // 不再需要这里创建extractor，已经提前创建了

            // Use the visitor to walk through the AST
            parsedSql.accept(extractor);

            // Print the results or do something with them

        } catch (SqlParseException e) {
            System.err.println("Error parsing SQL: " + e.getMessage());
        }

        // Return the results from the extractor
        return extractor.getTableFields();
    }


    private static class TableFieldExtractor extends SqlShuttle {
        private final Map<String, Set<String>> tableFields = new HashMap<>();
        private final Map<String, String> aliasToTableName = new HashMap<>();

        public Map<String, Set<String>> getTableFields() {
            // Convert alias to original table names
            Map<String, Set<String>> originalTableFields = new HashMap<>();
            for (Map.Entry<String, Set<String>> entry : tableFields.entrySet()) {
                String originalTableName = aliasToTableName.getOrDefault(entry.getKey(), entry.getKey());
                originalTableFields.computeIfAbsent(originalTableName, k -> new HashSet<>()).addAll(entry.getValue());
            }
            return originalTableFields;
        }

        @Override
        public SqlNode visit(SqlCall call) {
            if (call instanceof SqlSelect) {
                final SqlSelect select = (SqlSelect) call;
                handleSelect(select);
            } else if (call instanceof SqlJoin) {
                final SqlJoin join = (SqlJoin) call;
                handleJoin(join);
            }
            return super.visit(call);
        }

        private void handleSelect(SqlSelect select) {
            if (select.getFrom() != null) {
                select.getFrom().accept(this);
            }
        }

        private void handleJoin(SqlJoin join) {
            join.getLeft().accept(this);
            join.getRight().accept(this);
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            // Implementation of field extraction
            // ...
            return super.visit(id);
        }


    }



}
