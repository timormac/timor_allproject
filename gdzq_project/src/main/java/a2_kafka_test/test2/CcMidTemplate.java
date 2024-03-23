package a2_kafka_test.test2;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.time.DateFormatUtils;

public class CcMidTemplate {

    public String jsonData;
    public String jsonTemplate;
    public String occur_date;

    public String mobile ;




    public CcMidTemplate(String occur_date, String mobile) {
        this.jsonTemplate = "{\n" +
                "\"event_id\":\"019-1bbbb\",\n" +
                "\"event_name\":\"开户流程中断8min转化-cc（非视频见证）\",\n" +
                "\"client_name\":\"刘嘉浩\",\n" +
                "\"channel_name\":\"APP应用市场\",\n" +
                "\"branch_name\":\"\",\n" +
                "\"last_mobilenum\":\"555555\",\n" +
                "\"request_no\":\"2593832\",\n" +
                "\"business_name\":\"证件信息识别\",\n" +
                "\"step_code\":\"1\",\n" +
                "\"step_name\":\"上传身份证\",\n" +
                "\"channel_type\":\"znwh\",\n" +
                "\"mobile\":\"5555555555555\",\n" +
                "\"branch_code\":\" \",\"\n" +
                "occur_date\":\"20240321\",\n" +
                "\"occur_time\":\"15:29:28\",\n" +
                "\"birthday\":\"20050823\"\n" +
                "}";
        this.occur_date = occur_date;
        this.mobile = mobile;
        JSONObject jsonTemp = JSON.parseObject(this.jsonTemplate);

        jsonTemp.put("occur_date",occur_date);
        jsonTemp.put("mobile",mobile);
        this.jsonData = jsonTemp.toJSONString();
    }

    public void upsertAccourTime(){

        JSONObject json = JSON.parseObject(this.jsonData);
        //获得本地时间戳
        long stamp = System.currentTimeMillis();
        String date = DateFormatUtils.format(stamp, "HH:mm:ss");

        //System.out.println(date);
        json.put("occur_time",date);

        this.jsonData = json.toJSONString();


    }


    @Override
    public String toString() {
        return this.jsonData;
    }
}
