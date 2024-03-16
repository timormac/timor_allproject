package demand;

import com.alibaba.fastjson.JSONObject;

/**
 * @Title: A1_模拟数据_rt_crhkh_crh_wskh_userqueryextinfo
 * @Package: demand
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/16 00:00
 * @Version:1.0
 */
public class A1_模拟数据_rt_crhkh_crh_wskh_userqueryextinfo {

    public static void main(String[] args) {

        String json = "{\n" +
                "\"USER_ID\": \"3413602\",\n" +
                "\"MOBILE_TEL\": \"17832300656\",\n" +
                "\"BRANCH_NO\": \" \",\n" +
                "\"BUSINESS_FLAG_LAST\": \"22107\",\n" +
                "\"REQUEST_STATUS\": \"0\",\n" +
                "\"SUBMIT_DATETIME\": null,\n" +
                "\"CHANNEL_CODE\": \"10632\",\n" +
                "\"LAST_UPDATE_DATETIME\": \"2024-03-15 10:05:51\",\n" +
                "\"CLIENT_NAME\": \"刘嘉浩\",\n" +
                "\"ID_NO\": \"130421200508230659\",\n" +
                "\"BIRTHDAY\": \"20050823\",\n" +
                "\"REQUEST_NO\": \"2593832\"\n" +
                "}";

        JSONObject jsonObject = JSONObject.parseObject(json);
        System.out.println(  jsonObject.getString("BIRTHDAY"));


    }
}
