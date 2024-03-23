package a2_kafka_test.test1;

import org.apache.commons.lang3.time.DateFormatUtils;

public class Test {
    public static void main(String[] args) {

        long stamp = System.currentTimeMillis();
        String date = DateFormatUtils.format(stamp, "HH:mm:ss");

        System.out.println(date);
    }
}
