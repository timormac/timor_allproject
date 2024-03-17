package lpc.utils.gdzq.flinksql.dao;

import com.alibaba.fastjson.JSONObject;
import lpc.utils.mysql.dao.AbsctractMockDao;

/**
 * @Title: rt_crhkh_crh_wskh_userqueryextinfo
 * @Package: lpc.utils.mysql.gdzq.dao
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/16 08:08
 * @Version:1.0
 */
public class Rt_crhkh_crh_wskh_userqueryextinfo extends AbsctractMockDao {


    public int id;
    public String user_id;
    public String mobile_tel;
    public String branch_no;
    public String business_flag_last;
    public String request_status;
    public String submit_datetime;
    public String channel_code;
    public String last_update_detetime;
    public String client_name;
    public String id_no;
    public String birthday;
    public String request_no;

    //传一个json
    @Override
    public void mock(String... strings) throws InterruptedException {
        String jsonStr = strings[0];
        JSONObject json = JSONObject.parseObject(jsonStr);

        this.user_id = json.getString("user_id".toUpperCase());
        this.mobile_tel = json.getString("mobile_tel".toUpperCase());
        this.branch_no = json.getString("branch_no".toUpperCase());
        this.business_flag_last = json.getString("business_flag_last".toUpperCase());
        this.request_status = json.getString("request_status".toUpperCase());
        this.submit_datetime = json.getString("submit_datetime".toUpperCase());
        this.channel_code = json.getString("channel_code".toUpperCase());
        //this.last_update_detetime = json.getString("last_update_datetime".toUpperCase());
        this.last_update_detetime = strings[1];
        this.client_name = json.getString("client_name".toUpperCase());
        this.id_no = json.getString("id_no".toUpperCase());
        this.birthday = json.getString("birthday".toUpperCase());
        this.request_no = json.getString("request_no".toUpperCase());


    }

    @Override
    public String toString() {
        return "Rt_crhkh_crh_wskh_userqueryextinfo{" +
                "user_id='" + user_id + '\'' +
                ", mobile_tel='" + mobile_tel + '\'' +
                ", business_flag_last='" + business_flag_last + '\'' +
                ", last_update_detetime='" + last_update_detetime + '\'' +
                ", client_name='" + client_name + '\'' +
                '}';
    }
}
