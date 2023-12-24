package com.lpc.realtime.warehouse.a5_functions.dim;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lpc.realtime.warehouse.a2_utils.Tm_MysqlConnectPool;
import com.lpc.realtime.warehouse.a2_utils.Tm_MysqlUtils;
import com.lpc.realtime.warehouse.a2_utils.Tm_PhoenixConnectPool;
import com.lpc.realtime.warehouse.a2_utils.Tm_PhoenixUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


/**
 * @Author Timor
 * @Date 2023/12/23 18:21
 * @Version 1.0
 */

//CoProcessFunction<JSONObject, String, JSONObject>

public class A2_ConfigFilterTable  extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    DruidDataSource mysqlSource;
    DruidDataSource phoenixSouce;
    MapStateDescriptor<String, String> stateDescriptor;
    public A2_ConfigFilterTable( MapStateDescriptor<String, String> state ) {
        super();
        stateDescriptor = state;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        phoenixSouce = Tm_PhoenixConnectPool.getDruidDataSource();
        mysqlSource = Tm_MysqlConnectPool.getDruidDataSource();
        System.out.println("创建连接池");
    }


    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        BroadcastState<String, String> broadcastMapState = (BroadcastState<String, String>) ctx.getBroadcastState(stateDescriptor);
        String table = value.getString("table");

        System.out.println( "当前数据:"+value  );
        System.out.println("查看状态后端:当前map："+broadcastMapState);

        if( broadcastMapState.contains(table) ){
            String columes = broadcastMapState.get(table);
            String[] split = columes.split(",");
            JSONObject data = value.getJSONObject("data");
            Map<String, Object> innerMap = data.getInnerMap();
            //过滤字段
            mapFilterColumes(innerMap,split);
            String str = JSON.toJSONString(innerMap);
            JSONObject filterJson = JSON.parseObject(str);
            //把需要插入的表名放进去
            filterJson.put("sinktable", table);
            out.collect(filterJson);
        }



    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

        BroadcastState<String, String> broadcastMapState = ctx.getBroadcastState(stateDescriptor);
        JSONObject json = JSON.parseObject(value);
        JSONObject data = json.getJSONObject("data");
        String rowkey = data.getString("rowkey");
        String col = data.getString("columes");
        String table_name = data.getString("table_name");
        String[] columes = col.split(",");

        //配置表加入map状态中
        broadcastMapState.put(table_name,col);
        System.out.println( "map状态加入:" + table_name);

        DruidPooledConnection mysqlConn = mysqlSource.getConnection();
        DruidPooledConnection phoenixConn = phoenixSouce.getConnection();

        //获取mysql表字段类型
        HashMap<String, Object> map = Tm_MysqlUtils.getTableColumeForPhoenix(mysqlConn, table_name);

        //删除字段
        mapFilterColumes(map,columes);

        //创建hbase表
        Tm_PhoenixUtils.createHbaseTable(phoenixConn,"warehouse",table_name,map,rowkey);
        System.out.println("hbase创建表:"+table_name);

        mysqlConn.close();
        phoenixConn.close();

    }



    public void mapFilterColumes(Map<String,Object> map,String[] need_columes){
        ArrayList<String> list = new ArrayList<>();

        //将需要移除的字段放入list
        for (String keep : map.keySet()) {
            boolean  flag = true;
            for (String colume : need_columes) {
                if( colume.equals(keep) ){
                    flag = false;
                }
            }
            if(flag){
                list.add(keep);
            }
        }
        //移除字段
        for (String s1 : list) {
            map.remove(s1);
        }

    }

}
