package com.lpc.realtime.warehouse.a5_functions.dim;



import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lpc.realtime.warehouse.a2_utils.Tm_JsonTool;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/**
 * @Author Timor
 * @Date 2023/12/23 18:13
 * @Version 1.0
 */
public class A1_FilterConfig extends ProcessFunction<String, JSONObject> {

    OutputTag<String> wrongData;

    OutputTag<String> config;

    public A1_FilterConfig(OutputTag<String> wrongData, OutputTag<String> config) {
        super();
        this.wrongData = wrongData;
        this.config = config;
    }

    @Override
    public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        if (!Tm_JsonTool.isJSON(value)) {
            System.out.println("非json数据测输出流:" + value);
            ctx.output(wrongData, value);

            //如果是非json直接跳出
            return;
        }

        com.alibaba.fastjson.JSONObject valueJson = JSON.parseObject(value);

        if ("flink_warehouse_db".equals(valueJson.getString("database"))) {

            if ("flinkconfig".equals(valueJson.getString("table")) && "insert".equals(  valueJson.getString("type") )  ) {
                ctx.output(config, value);
                System.out.println("config侧输出流:" + value);
            } else {
                out.collect(valueJson);
            }
        }
    }
}
