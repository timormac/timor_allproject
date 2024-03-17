package lpc.utils.gdzq.sqlanalyse;

import com.opencsv.CSVReader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

/**
 * @Title: CSVTool
 * @Package: lpc.utils.gdzq.sqlanalyse
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/17 10:48
 * @Version:1.0
 */
public class CSVTool {

    public static ArrayList<String> getColumeList( String filePath,String rowName ) throws IOException {

        CSVReader reader = new CSVReader(new FileReader("/Users/timor/Desktop/未命名文件夹/元数据变更风险评估.csv"));
        String[] nextLine;

        //获取第一行字段列表
         nextLine = reader.readNext();

         int colIndex = -1;

         //获取字段下标位置
        for (int i = 0; i < nextLine.length; i++) {
            if( rowName.equals( nextLine[i] ) ){
                colIndex = i;
                break;
            }

        }

        ArrayList<String> list = new ArrayList<>();

        while( (nextLine = reader.readNext()) != null  ){
           list.add(nextLine[colIndex]) ;
        }

        return  list;
    }



}
