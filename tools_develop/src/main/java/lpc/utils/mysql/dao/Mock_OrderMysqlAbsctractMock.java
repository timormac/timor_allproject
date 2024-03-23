package lpc.utils.mysql.dao;

import java.sql.Timestamp;
import java.util.Random;

/**
 * @Author Timor
 * @Date 2024/2/24 18:42
 * @Version 1.0
 */
public class Mock_OrderMysqlAbsctractMock extends Mysql_AbsctractMockDao {

    public int id;
    public String orderno;
    public String userid;
    public int spuid;
    public double price;
    public Timestamp create_time;
    public int status;


    @Override
    public void mock(String... strings) throws InterruptedException {
        Random random = new Random();
        long l = System.currentTimeMillis();
        orderno = "D" + String.valueOf(l).substring(4)+"|"+random.nextInt(10);
        //userid = "user" + random.nextInt(101);
        userid = "user" + random.nextInt(10);
        //spuid = random.nextInt(101); // 生成一个0到100之间的随机整数
        spuid = random.nextInt(2); // 生成一个0到100之间的随机整数
        price = random.nextDouble()*1000;
        create_time = new Timestamp(l);
        status=random.nextInt(3);
    }

    @Override
    public String toString() {
        return "MockOrder{" +
                "id=" + id +
                ", orderno='" + orderno + '\'' +
                ", userid='" + userid + '\'' +
                ", spuid=" + spuid +
                ", price=" + price +
                ", create_time=" + create_time +
                ", status=" + status +
                "} " + super.toString();
    }
}
