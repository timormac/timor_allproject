package com.timor.timor_develop_tools.hive;

import com.alibaba.druid.pool.DruidDataSource;
import com.lpc.datamock.tools.A0_PropertyUtils;

import java.io.IOException;
import java.sql.*;
import java.util.*;

/**
 * @Title: MysqlTableSyncHive
 * @Package: com.timor.timor_develop_tools.hive
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/2 18:19
 * @Version:1.0
 */
public class MysqlTableSyncHive {

    private DruidDataSource source;
    private Connection connection;
    private  Connection hiveConnect ;
    private  Statement stmt ;
    private Properties property ;

    public MysqlTableSyncHive() throws IOException {
        this.property = new A0_PropertyUtils("jdbc.properties").getProperty();
        getConnecPool();
    }

    DruidDataSource getConnecPool() throws IOException {
         this.source= new DruidDataSource();
         source.setDriverClassName(property.getProperty("driver"));
         source.setUrl(property.getProperty("url"));
         source.setUsername(property.getProperty("user"));
         source.setPassword(property.getProperty("password"));
         //一开始提前申请好5个连接，不够了，重写申请
         source.setInitialSize( Integer.parseInt(property.getProperty("initialSize"))  );
         //最多不超过10个，如果10都用完了，还没还回来，就会出现等待
         source.setMaxActive(Integer.parseInt(property.getProperty("MaxActive")) );
         //用户最多等1000毫秒，如果1000毫秒还没有人还回来，就异常了
         source.setMaxWait(Integer.parseInt(property.getProperty("MaxWait"))  );
         return this.source;
     }

     void getConnect() throws SQLException {
         this.connection=this.source.getConnection();
     }


    Map<String, String> getColumns(String tableName) throws SQLException {

         Map<String, String> map = new LinkedHashMap<String, String>();
         getConnect();
         String sql = "describe  "+ tableName ;
         PreparedStatement pst = this.connection.prepareStatement(sql);
         ResultSet resultSet = pst.executeQuery();
         while ( resultSet.next() ){
             String field = resultSet.getString("field");
             String type = resultSet.getString("type");
             map.put(field,type);
         }

         pst.close();
         connection.close();
         return map;
     }

     String columnTypeTransform(String type) throws Exception {

         String[] split = type.split("\\(");
         String name = split[0];
         String transform = null;

         if("varchar".equals(name)) transform = "String";
         else if("char".equals(name)) transform = "String";
         else if("text".equals(name)) transform = "String";
         else if("float".equals(name)) transform = "float";
         else if("double".equals(name)) transform = "double";
         else if("decimal".equals(name)) transform = "decimal(10,2)";
         else if("date".equals(name)) transform = "date";
         else if("timestamp".equals(name)) transform = "timestamp";
         else if("datetime".equals(name)) transform = "timestamp";
         else if("tinyint".equals(name)) transform = "tinyint";
         else if("smallint".equals(name)) transform = "smallint";
         else if("int".equals(name)) transform = "int";
         else if("bigint".equals(name)) transform = "bigint";
         else throw new Exception("mysql数据类型未匹配");
         return transform;
     }

     void hiveClientInit() throws ClassNotFoundException, SQLException {
         Class.forName("org.apache.hive.jdbc.HiveDriver");
         String hiveUrl =property.getProperty("hiveUrl");
         String hiveUser = property.getProperty("hiveUser");
         String hivePassword = property.getProperty("hivePassword");
         hiveConnect = DriverManager.getConnection(hiveUrl, hiveUser, hivePassword);
         stmt = hiveConnect.createStatement();

     }


     String getHiveSql(String tableName) throws Exception {
         return getHiveSql(tableName, "","");
     }

    String getHiveSql(String tbName,String prefixName,String location) throws Exception {
        Map<String, String> map = getColumns(tbName);
        String hiveTableName ;
        if(prefixName.length()==0)  hiveTableName = tbName;
        else hiveTableName = prefixName + "_" + tbName;
        String head ="create external table if not exists  " + hiveTableName + " ( "       ;
        String mid = "" ;
        int tag=0;
        String need ="";
        for (String key : map.keySet()) {
            if(tag>0) need = ",";
            String value = columnTypeTransform(map.get(key));
            mid +=  need  + "  " + key + "  " + value +" \n ";
            tag++;
        }

        String tail= " ) "  +
                "row format delimited fields terminated by '\\t' " +
                "lines terminated by '\\n' " +
                "stored as textfile \n";

        if(location.length()>0) tail += " location  '" + location + "/" +hiveTableName + "'" ;

        String sql = head +mid +tail;
        System.out.println(sql);
        return sql;
    }

     void hiveCreateTable(String tbName,String prefixName,String location) throws Exception {
         Class.forName("org.apache.hive.jdbc.HiveDriver");
         hiveClientInit();
         String sql = getHiveSql(tbName,prefixName,location);
         stmt.execute(sql);
     }
    void hiveCreateTable(String tbName) throws Exception {
        hiveCreateTable(tbName,"","");
    }


     List<String> getTableList() throws SQLException {
         getConnect();
         ArrayList<String> tableNameList = new ArrayList<String>();
         String sql = "show tables" ;
         PreparedStatement pst = this.connection.prepareStatement(sql);
         ResultSet resultSet = pst.executeQuery();
         while ( resultSet.next() ){
             String tableName = resultSet.getString(1);
             System.out.println(tableName);
             tableNameList.add(tableName);
         }
         return tableNameList;

     }

    void hiveCreateTableBatch(String prefixName,String location) throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        hiveClientInit();
        List<String> list = getTableList();
        for (String name : list) hiveCreateTable(name,prefixName,location);
        stmt.close();
        connection.close();
    }

    void hiveCreateTableBatch(String prefixName) throws Exception {
        hiveCreateTableBatch(prefixName,"");
    }








}
