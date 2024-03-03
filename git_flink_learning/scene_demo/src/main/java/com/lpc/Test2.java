package com.lpc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.formats.csv.CsvRowDataDeserializationSchema;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.hadoop.fs.Path;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author Timor
 * @Date 2024/3/1 12:25
 * @Version 1.0
 */
public class Test2 {
    public static void main(String[] args) throws Exception {
        String element="2021-12-31 17:35:9.522";


        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        Date parse = null;
        try {
            parse = dateFormat.parse(element);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        System.out.println(parse.getTime());


    }
}
