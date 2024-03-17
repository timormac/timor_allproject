package lpc.utils.gdzq.sqlanalyse;

import com.opencsv.CSVWriter;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;

/**
 * @Title: 风险评估表字段数据合并
 * @Package: lpc.utils.gdzq.sqlanalyse
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/17 17:05
 * @Version:1.0
 */
public class 风险评估表字段数据合并 {

    public static void main(String[] args) throws IOException {

        //String fileName = "/Users/timor/Desktop/未命名文件夹/123";
        String fileName = "/Users/timor/Desktop/未命名文件夹/表涉及字段";
        BufferedReader br = new BufferedReader(new FileReader(fileName));

        HashMap<String, HashSet<String>> map = new HashMap<>();

        String line;

        while ( (line = br.readLine()) != null ) {

            String[] split = line.split(":");

            //过滤掉数字和空行
            if(split.length==2){
                //库/表名统一大写,表/库的""去掉,把多余的空格去掉
                String tableName = split[0].toUpperCase().replace("\"","").replace(" ","");
                String[] fieldArr = split[1].split(",");

                if( !map.containsKey(tableName) ){
                    HashSet<String> set = new HashSet<>();
                    for (String field : fieldArr) {
                        //去掉字段空格
                        set.add(field.replace(" ",""));
                    }
                    map.put(tableName,set);

                }else {
                    HashSet<String> set = map.get(tableName);
                    for (String field : fieldArr) {
                        //去掉字段空格
                        set.add(field.replace(" ",""));
                    }
                }
            }

        }


        HashMap<String, HashSet<String>> needMap = new HashMap<>();

        //过滤CRH的表
        for (String table : map.keySet()) {

            if(table.contains("CRH")){
                needMap.put(table,map.get(table));
            }
        }


        for (String table : needMap.keySet()) {
            String res = table + " : ";
            HashSet<String> set = needMap.get(table);

            int flag = 0 ;

            for (String feild : set) {
                flag +=1 ;
                if(  flag < set.size() ){
                    res += feild + "," ;
                }else {
                    res += feild;
                }
            }

           // System.out.println(res);

        }


        String csvFile = "/Users/timor/Desktop/未命名文件夹/接口所用列.csv";

        CSVWriter writer = new CSVWriter(new FileWriter(csvFile)) ;
        String[] header = {"table", "feilds"};
        writer.writeNext(header);

        for (String table : needMap.keySet()) {

            HashSet<String> set = needMap.get(table);
            int flag = 0 ;
            String fields = "";
            String[] arr = new String[2];

            for (String feild : set) {
                flag +=1 ;
                if(  flag < set.size() ){
                    fields += feild + "," ;
                }else {
                    fields += feild;
                }
            }

            arr[0] = table;
            arr[1] = fields;
            System.out.println(arr);
            writer.writeNext( arr);
        }
        writer.close();


    }
}
