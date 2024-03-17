package lpc.utils.gdzq.sqlanalyse;

import com.opencsv.CSVWriter;

import java.io.FileWriter;
import java.io.IOException;

/**
 * @Title: TTest
 * @Package: lpc.utils.gdzq.sqlanalyse
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/17 17:38
 * @Version:1.0
 */
public class TTest {
    public static void main(String[] args) throws IOException {

        String csvFile = "/Users/timor/Desktop/未命名文件夹/12345.csv";

//        try (CSVWriter writer = new CSVWriter(new FileWriter(csvFile))) {
//            // Writing data to CSV file
//            String[] header = {"Name", "Age", "Country"};
//            writer.writeNext(header);
//
//            String[] data1 = {"Alice", "25", "USA"};
//            writer.writeNext(data1);
//
//            String[] data2 = {"Bob", "30", "Canada"};
//            writer.writeNext(data2);
//
//            System.out.println("CSV file created successfully.");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }



        CSVWriter writer = new CSVWriter(new FileWriter(csvFile)) ;
        String[] header = {"table", "feilds"};
        writer.writeNext(header);

        String[] data2 = {"Bob", "30"};
        writer.writeNext(data2);

        System.out.println("CSV file created successfully.");
        writer.close();

    }
}
