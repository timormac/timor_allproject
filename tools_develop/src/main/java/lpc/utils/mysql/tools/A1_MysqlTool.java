package lpc.utils.mysql.tools;

import com.alibaba.druid.pool.DruidDataSource;
import lpc.utils.mysql.dao.Dao;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * @Author Timor
 * @Date 2024/2/24 16:43
 * @Version 1.0
 */
public class A1_MysqlTool {

    private static DruidDataSource druidDataSource ;

    static {
        druidDataSource = new DruidDataSource();
        druidDataSource.setDriverClassName(A1_ConfigProperty.MYSQL_DRIVER);
        druidDataSource.setUrl(A1_ConfigProperty.MYSQL_URL);
        druidDataSource.setUsername(A1_ConfigProperty.MYSQL_USER);
        druidDataSource.setPassword(A1_ConfigProperty.MYSQL_PASSWORD);

        // 设置初始化连接池时池中连接的数量
        druidDataSource.setInitialSize(5);
        // 设置同时活跃的最大连接数
        druidDataSource.setMaxActive(10);
        // 设置空闲时的最小连接数，必须介于 0 和最大连接数之间，默认为 0
        druidDataSource.setMinIdle(1);
        // 设置没有空余连接时的等待时间，超时抛出异常，-1 表示一直等待
        druidDataSource.setMaxWait(-1);
        // 验证连接是否可用使用的 SQL 语句
        druidDataSource.setValidationQuery("select 1");
        // 指明连接是否被空闲连接回收器（如果有）进行检验，如果检测失败，则连接将被从池中去除
        // 注意，默认值为 true，如果没有设置 validationQuery，则报错
        // testWhileIdle is true, validationQuery not set
        druidDataSource.setTestWhileIdle(true);
        // 借出连接时，是否测试，设置为 false，不测试，否则很影响性能
        druidDataSource.setTestOnBorrow(false);
        // 归还连接时，是否测试
        druidDataSource.setTestOnReturn(false);
        // 设置空闲连接回收器每隔 30s 运行一次
        druidDataSource.setTimeBetweenEvictionRunsMillis(30 * 1000L);
        // 设置池中连接空闲 30min 被回收，默认值即为 30 min
        druidDataSource.setMinEvictableIdleTimeMillis(30 * 60 * 1000L);

        System.out.println("Druid线程池初始化");

    }

    //获取mysql线程池连接
    public static Connection getPoolConnect() throws SQLException {
        System.out.println("获取mysql连接");
        return druidDataSource.getConnection();

    }


    //获取mysql表字段类型map<字段名,类型>
    public static HashMap< String, String > getTableColume(Connection connect, String tableName) throws SQLException {
        LinkedHashMap<String, String > map = new LinkedHashMap<>();
        String sql = "describe  " + tableName ;
        PreparedStatement preparedStatement = connect.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();

        while ( resultSet.next() ) {
            String field = resultSet.getString("field");
            String type = (resultSet.getString("type").split("\\("))[0];



            map.put(field,type);
        }
        preparedStatement.close();
        return  map;
    }


    //按表名查询,需要过滤条件直接写在filterCondition例如：id = "dasd"
    public static ArrayList<Dao> tableQueryset(Connection connect, String tb, String clsName, String filterCondition) throws Exception {

        HashMap<String, String > tableColumeMap = getTableColume(connect, tb);
        String filterSql =  filterCondition == "" ? "" : " where " + filterCondition;
        String sql = "select  * from " + tb + filterSql;
        Statement statement = connect.createStatement();
        System.out.println(sql);

        System.out.println(tableColumeMap);

        ResultSet resultSet = statement.executeQuery(sql);

        ArrayList<Dao> dao_arr = new ArrayList<>();

        //String className = "com.lpc.datamock.dao.Spu";
        Class<?> clazz = Class.forName(clsName);
        //获取属性个数
        Field[] fields = clazz.getDeclaredFields();

        ArrayList<String> arr = new ArrayList<>();

        HashMap<String, Object> daoMap = new HashMap<>();

        while ( resultSet.next() ){
            for (String name : tableColumeMap.keySet()) {
                String columnType = tableColumeMap.get(name);
                if( "varchar".equals(columnType) ){
                    daoMap.put(name,resultSet.getString(name));
                }else if( "bigint".equals(columnType) ){
                    daoMap.put(name,resultSet.getInt(name));
                }else if( "decimal".equals(columnType) ){
                    daoMap.put(name,resultSet.getDouble(name));
                }else if( "datetime".equals(columnType) ){
                    daoMap.put(name,resultSet.getTimestamp(name));
                }else if( "timestamp".equals(columnType) ){
                    daoMap.put(name,resultSet.getTimestamp(name));
                }else {
                    throw new Exception("tableQueryset:方法类型不匹配");
                }
            }
            Object object = clazz.newInstance();
            Dao dao = (Dao) object ;
            dao.getInstance(daoMap);
            dao_arr.add(dao);
        }

        return dao_arr;
    }

    //将Dao对象的list批量写入mysql
    public static void insertTableDaoList(Connection connect, String tb, String clsName, List<Dao> daoList) throws Exception {

        Class<?> clazz = Class.forName(clsName);

        //获取Dao属性个数
        int length = clazz.getDeclaredFields().length;
        Field[] fields = clazz.getDeclaredFields();

        String sql = "insert into " + tb + " ( ";

        for (int i = 0; i < length; i++) {

            if( i == length -1){
                sql += fields[i].getName() + ") ";
            } else{
                sql += fields[i].getName() + ",";
            }
        }

        sql += " values(";

        for (int i = 0; i < length; i++) {
            if( i == length -1){
                sql += "?) ";
            } else{
                sql += "?,";
            }
        }

      // System.out.println( sql );

        //这里先传sql,mysl服务器会对这个sql预编译
        PreparedStatement pst = connect.prepareStatement(sql);

        //自增主键无所谓,目前所有id都是0，写入mysql还是自动生成
        for (Dao dao : daoList) {
            //检测dao是不是真的cls实例
            if( !(clazz.isInstance(dao)) ) {
                throw new Exception("实例错误");
            }
            Object cast = clazz.cast(dao);
            for (int i = 0; i < fields.length; i++) {
                Field field = fields[i];
                Class<?> type = field.getType();
                if( type.equals(String.class) ){
                    pst.setString(i+1,(String)field.get(cast) );
                }else if( type.equals(int.class) ){
                    pst.setInt(i+1,(int)field.get(cast) );
                }else if( type.equals(Integer.class) ){
                    pst.setInt(i+1,(int)field.get(cast) );
                }else if( type.equals(Double.class) ){
                    pst.setDouble(i+1,(double)field.get(cast) );
                }else if( type.equals(double.class) ){
                    pst.setDouble(i+1,(double)field.get(cast) );
                }else if( type.equals(Timestamp.class) ){
                    pst.setTimestamp(i+1,(Timestamp)field.get(cast)  );
                }else {
                    throw new Exception("有异常mysql数据类型");
                }
            }
            pst.addBatch();
        }
        pst.executeBatch();
        pst.close();
    }

    //获取mysql表,映射类的代码,减少手动创建dao类的代码，直接复制黏贴
    public static void tableToObjectCode(Connection connect, String tb) throws Exception {

        HashMap<String, String> map = getTableColume(connect, tb);
        for (String key : map.keySet()) {
            String type = map.get(key);
            if( "varchar".equals(type) ){
                System.out.println( "public" + " String " + key + ";" );
            }else if( "bigint".equals(type) ){
                System.out.println( "public" + " int " + key + ";" );
            }else if( "decimal".equals(type) ){
                System.out.println( "public" + " double " + key + ";" );
            }else if( "datetime".equals(type) ){
                System.out.println( "public" + " Timestamp " + key + ";" );
            }else if( "timestamp".equals(type) ){
                System.out.println( "public" + " Timestamp " + key + ";" );
            }else {
                throw new Exception("tableQueryset:方法类型不匹配");
            }

        }

    }


}
