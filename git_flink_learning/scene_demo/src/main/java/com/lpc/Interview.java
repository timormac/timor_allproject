package com.lpc;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

/**
 * @Author Timor
 * @Date 2024/3/1 10:59
 * @Version 1.0
 */
public class Interview {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Path path = new Path("C:\\Users\\lpc\\Desktop\\google下载目录\\simple.etl.csv");
        //Path path = new Path("C:\\Users\\lpc\\Desktop\\google下载目录\\temp.csv");
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), path).build();

        DataStreamSource<String> fileDS = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file");

        SingleOutputStreamOperator<UserLog> flatmap = fileDS.flatMap(new FlatMapFunction<String, UserLog>() {

            Integer count = 0;

            @Override
            public void flatMap(String value, Collector<UserLog> out) throws Exception {

                count++;
                String[] split = value.split(",\"\\{");

                //过滤这类数据2021-12-31 17:35:11.394,,,,,,,,,,格式数据
                String json ="";
                if(split.length>1){
                    String  info1 = split[0];
                    String[] infoSplit = info1.split(",");

                    //过滤缺少字段数据
                    if(infoSplit.length==7){
                        //清洗json格式
                        String  parameterDetail = split[1];
                        parameterDetail = value.split("\\{")[1];
                        String r1 = parameterDetail.replace("\"\"ismain\"\":1\"", "\"ismain\":1");
                        String r2 = r1.replace("\"\"ismain\"\":0\"", "\"ismain\":0");
                        json = "{" + r2;
                        Interview.UserLog userLog = new Interview.UserLog( infoSplit[0],infoSplit[1] ,infoSplit[2],infoSplit[3],infoSplit[4],infoSplit[5],infoSplit[6],json);
                        out.collect(userLog);
                    }else {
                       // System.out.println("脏数据:"+value);
                    }

                }else {
                   // System.out.println("脏数据:"+value);
                }
            }
        });

        WatermarkStrategy<UserLog> strategy = WatermarkStrategy
                .<UserLog>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<UserLog>() {
                    @Override
                    public long extractTimestamp(UserLog element, long recordTimestamp) {
                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

                        Date parse = null;
                        try {
                            parse = dateFormat.parse(element.createTime);
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        return parse.getTime();
                    }
                });

        SingleOutputStreamOperator<UserLog> watermarkDS = flatmap.assignTimestampsAndWatermarks(strategy);

        KeyedStream<UserLog, String> level = watermarkDS.keyBy(new KeySelector<UserLog, String>() {
            @Override
            public String getKey(UserLog value) throws Exception {
                JSONObject json = JSONObject.parseObject(value.parameterDetail);
                return json.getString("level");
            }
        });

        WindowedStream<UserLog, String, TimeWindow> window = level.window(TumblingEventTimeWindows.of(Time.seconds(3L)));

        window.process(new ProcessWindowFunction<UserLog, String, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<UserLog> elements, Collector<String> out) throws Exception {
                long start = context.window().getStart();
                int count= 0;
                for (UserLog element : elements) {
                    count++;
                }
                String result = "TimeWindow:"+start+" key:"+s +" count:"+count;

                out.collect(result);

            }
        }).print();

        env.execute();



    }



    static class UserLog{

        public String createTime;
        public String userid;
        public String productCode;
        public  String funcitonCode;
        public String mail;
        public String City;
        public String  costTime;
        public String parameterDetail;

        public UserLog(String createTime, String userid, String productCode, String funcitonCode, String mail, String city, String costTime, String parameterDetail) {
            this.createTime = createTime;
            this.userid = userid;
            this.productCode = productCode;
            this.funcitonCode = funcitonCode;
            this.mail = mail;
            City = city;
            this.costTime = costTime;
            this.parameterDetail = parameterDetail;
        }

        public UserLog() {
        }

        @Override
        public String toString() {
            return "createTime=" + createTime +
                    ", userid='" + userid + '\'' +
                    ", costTime=" + costTime +
                    ", parameterDetail="+parameterDetail;
        }
    }

}

