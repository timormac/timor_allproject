package com;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.util.ArrayList;
import java.util.Calendar;

/**
 * @Title: Tst
 * @Package: com
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/17 17:04
 * @Version:1.0
 */
public class Test {

    public static void main(String[] args) {


        Calendar calendar = Calendar.getInstance();

        int year = calendar.get(Calendar.YEAR);

        int month = calendar.get(Calendar.MONTH);

        int date = calendar.get(Calendar.DATE);

        int hour = calendar.get(Calendar.HOUR_OF_DAY);

        int minute = calendar.get(Calendar.MINUTE);

        int second = calendar.get(Calendar.SECOND);

        System.out.println( year+"-"+month+"-"+date+"-"+hour+"-"+minute);


    }
}
