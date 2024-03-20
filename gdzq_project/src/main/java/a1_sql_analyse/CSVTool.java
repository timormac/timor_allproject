package a1_sql_analyse;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Title: CSVTool
 * @Package: lpc.utils.gdzq.sqlanalyse
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/17 10:48
 * @Version:1.0
 */
public class CSVTool {


    /**
     * 获取csv文件的指定列,返回一个list,第一行必须是表结构
     * @param filePath   csv文件路径
     * @param rowName    指定列名
     * @return
     * @throws IOException
     */
    public static ArrayList<String> getColumeList( String filePath,String rowName ) throws IOException {
        // filePath ="/Users/timor/Desktop/未命名文件夹/元数据变更风险评估.csv"


        CSVReader reader = new CSVReader(new FileReader(filePath));
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

    /**
     * 生成 一个csv文件
     * @param filePath
     * @param dataList
     * @param rowNameList
     * @throws IOException
     */
    public static void createCSVFile(String filePath, List<String[]> dataList, String... rowNameList ) throws IOException {
        //filePath = "/Users/timor/Desktop/未命名文件夹/12345.csv";

        CSVWriter writer = new CSVWriter(new FileWriter(filePath)) ;

        //首行插入列名称
        writer.writeNext(rowNameList);

        //插入数据
        for (String[] row : dataList) {
            writer.writeNext(row);
        }

        //关闭writer不然的化，生成的csv是空的
        writer.close();

        System.out.println("CSV file created successfully.");

    }



}
