package lpc.utils.mysql.dao;

import java.sql.Timestamp;
import java.util.Random;

/**
 * @Author Timor
 * @Date 2024/2/24 21:04
 * @Version 1.0
 */
public class Mock_Cupon extends AbsctractMockDao{

    public int id;
    public String cuponno;
    public String orderno;
    public double money;
    public Timestamp create_time;
    public String activity;

    @Override
    public void mock(String... strings) throws InterruptedException {
        Random random = new Random();
        String[] activityArr= new String[]{ "新人活动","春节活动","双十一活动","优惠红包"  } ;
        orderno = strings[0];
        long l = System.currentTimeMillis();
        cuponno = "CUP" + String.valueOf(l).substring(4)+"|"+random.nextInt(10);
        money = random.nextDouble()*20;
        create_time = new Timestamp(l);
        activity=  activityArr[random.nextInt(4)] ;
    }

    @Override
    public String toString() {
        return "Mock_Cupon{" +
                "id=" + id +
                ", cuponno='" + cuponno + '\'' +
                ", orderno='" + orderno + '\'' +
                ", money=" + money +
                ", create_time=" + create_time +
                ", activity='" + activity + '\'' +
                "} " + super.toString();
    }
}
