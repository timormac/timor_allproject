package a2_kafka_test.a1_user_create_interrupt;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.time.DateFormatUtils;

/**
 * @Title: DataTemplate
 * @Package: PACKAGE_NAME
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/18 06:45
 * @Version:1.0
 */
public class DataTemplate {

    String jsonData;

    String jsonTemplate;

    String businessLastFlag;

    Integer delaySeconds;


    public DataTemplate(  String  mobileTel,String businessLastFlag, Integer delaySeconds) {
        this.businessLastFlag = businessLastFlag;
        this.delaySeconds = delaySeconds;
        this.jsonTemplate = "{\n" +
                "    \"message\": {\n" +
                "        \"data\": {\n" +
                "            \"USER_ID\": \"3413602\",\n" +
                "            \"MOBILE_TEL\": \"17832300656\",\n" +
                "            \"BRANCH_NO\": \" \",\n" +
                "            \"BUSINESS_FLAG_LAST\": \"22107\",\n" +
                "            \"REQUEST_STATUS\": \"0\",\n" +
                "            \"SUBMIT_DATETIME\": null,\n" +
                "            \"CHANNEL_CODE\": \"10632\",\n" +
                "            \"LAST_UPDATE_DATETIME\": \"2024-03-15 10:05:51\",\n" +
                "            \"CLIENT_NAME\": \"刘嘉浩\",\n" +
                "            \"ID_NO\": \"130421200508230659\",\n" +
                "            \"BIRTHDAY\": \"20050823\",\n" +
                "            \"REQUEST_NO\": \"2593832\"\n" +
                "        }\n" +
                "    }\n" +
                "}\n";

        JSONObject json = JSON.parseObject(this.jsonTemplate);

        JSONObject message = json.getJSONObject("message");
        JSONObject data = message.getJSONObject("data");

        //替换flag和mobileTel
        data.put("MOBILE_TEL",mobileTel);
        data.put("BUSINESS_FLAG_LAST",businessLastFlag);

        this.jsonData = json.toJSONString();

    }

    //每次sleep后更细LAST_UPDATE_DATETIME
    void upsertDatetime(){

        JSONObject json = JSON.parseObject(this.jsonData);

        JSONObject message = json.getJSONObject("message");
        JSONObject data = message.getJSONObject("data");

        //获得本地时间戳
        long stamp = System.currentTimeMillis();
        String date = DateFormatUtils.format(stamp, "yyyy-MM-dd HH:mm:ss");

        //System.out.println(date);
        data.put("LAST_UPDATE_DATETIME",date);

        this.jsonData = json.toJSONString();


    }


}
