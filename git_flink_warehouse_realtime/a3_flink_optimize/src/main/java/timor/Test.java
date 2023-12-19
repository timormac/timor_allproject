package timor;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.java.tuple.Tuple4;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Random;

/**
 * @Author Timor
 * @Date 2023/12/18 20:40
 * @Version 1.0
 */
public class Test {

    public static void main(String[] args) throws Exception {

        Random random = new Random();

        double money = Math.random()*1000;
        String id = "" + random.nextInt(5000) ;
        String s="{\"address\":\"北京市\",\"age\":20,\"name\":\"张三\"" +
                ",\"id\":"+ "\""+id +"\"" +
                ",\"money\":"+ money+
                "}" ;

        System.out.println(s);


        System.out.println(JSON.parseObject(s).getString("money"));
        System.out.println(JSON.parseObject(s).getString("id"));

    }


}
