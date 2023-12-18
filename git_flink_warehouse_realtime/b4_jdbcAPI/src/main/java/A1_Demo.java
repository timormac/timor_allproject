import java.io.IOException;
import java.sql.*;

/**
 * @Title: Test
 * @Package: PACKAGE_NAME
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/20 13:40
 * @Version:1.0MysqlAPI
 */
public class A1_Demo {

    Connection  connect ;
    public A1_Demo() throws SQLException, IOException {
        A3_ConnectPool pool = new A3_ConnectPool();
        this.connect = pool.getPoolConnect();
    }

    public static void main(String[] args) throws SQLException, IOException {

        A1_Demo test = new A1_Demo();
        test.preparedInsertBatch();

    }

    void preparedInsert() throws SQLException {

        String sql = "insert into spu(spu_name,price,merchantid,create_time,modify_time) values(?,?,?,?,?)";
        //这里先传sql,mysl服务器会对这个sql预编译
        PreparedStatement pst = this.connect.prepareStatement(sql);

        pst.setString(1,"bbbb");
        pst.setDouble(2,8.5);
        pst.setInt(3,2);
        //注意这里要timestamp,别用setdate,不然没有时分秒
        Timestamp timestamp = new Timestamp( System.currentTimeMillis() );
        pst.setTimestamp(4,timestamp);
        pst.setTimestamp(5,timestamp);
        boolean execute = pst.execute();
        System.out.println(execute);
        pst.close();
        connect.close();
    }

    void preparedInsertBatch() throws SQLException {

        String sql = "insert into spu(spu_name,price,merchantid,create_time,modify_time) values(?,?,?,?,?)";
        //这里先传sql,mysl服务器会对这个sql预编译
        PreparedStatement pst = this.connect.prepareStatement(sql);

        for (int i = 0; i <10 ; i++) {
            pst.setString(1,"bbbb");
            pst.setDouble(2,8.5);
            pst.setInt(3,2);
            //注意这里要timestamp,别用setdate,不然没有时分秒
            Timestamp timestamp = new Timestamp( System.currentTimeMillis() );
            pst.setTimestamp(4,timestamp);
            pst.setTimestamp(5,timestamp);

            //增加成一批，一次处理
            pst.addBatch();
        }

        pst.executeBatch();
        pst.close();
        connect.close();

    }


    void preparedQuery() throws SQLException {
        String sql = "select * from spu where id= ? ";
        PreparedStatement pst = this.connect.prepareStatement(sql);
        int id = 3;
        pst.setObject(1,3);
        ResultSet resultSet = pst.executeQuery();
        resultSet.next();
        String spu_name = resultSet.getString("spu_name");
        System.out.println(spu_name);
        pst.close();
        connect.close();

    }



    //这种方式不好用，不防sql注入，并且自己不知道datetime的格式导致插入sql执行不了
    void sqlInsert() throws SQLException {

        //Class.forName("com.mysql.jdbc.Driver"); 加载驱动
        Statement statement = this.connect.createStatement();
        String sql = "insert into spu values(" +"'name1',8.5,1)";
        boolean execute = statement.execute(sql);
        System.out.println(execute);

        statement.close();
        connect.close();
    }

    //不防sql注入，不用
    void sqlQuery() throws SQLException {
        Statement statement = this.connect.createStatement();
        String sql="select * from spu";
        ResultSet resultSet = statement.executeQuery(sql);

        while(resultSet.next()){
            String spu_name = resultSet.getString("spu_name");
            Date create_time = resultSet.getDate("create_time");
            System.out.println(spu_name+create_time);
        }
        resultSet.close();
        statement.close();
        connect.close();

    }


}


