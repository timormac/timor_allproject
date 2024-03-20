package a1_sql_analyse;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @Title: Test
 * @Package: a1_sql_analyse
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/21 03:14
 * @Version:1.0
 */
public class Test {
    public static void main(String[] args) throws IOException {

        String filePath = "/Users/timor/Desktop/未命名文件夹/12345.csv";

        ArrayList<String[]> list = new ArrayList<>();
        String[]  arr = new String[]{"a","b"};
        String[]  arr2 = new String[]{"c","d"};
        list.add(arr);
        list.add(arr2);


        CSVTool.createCSVFile(filePath,list,"name","age");

    }
}
