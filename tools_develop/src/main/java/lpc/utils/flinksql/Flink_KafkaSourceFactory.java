package lpc.utils.flinksql;

import java.util.*;

/**
 * @Title: A1_KafkaSource
 * @Package: tools.flinksource
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/23 02:39
 * @Version:1.0
 */
public class Flink_KafkaSourceFactory {
    public static  KafkaSource  getKafkaSource(){
        return new KafkaSource()  ;
    }

    public static  class KafkaSource{
        public  TopicedKafkaSource setTopicAndTable(String topic,String tableName){
            return new TopicedKafkaSource(topic,tableName);
        }
    }
    public static class TopicedKafkaSource{
        private  String topic ;
        private  String tableName ;
        public TopicedKafkaSource(String topic,String tableName) {
            this.topic = topic;
            this.tableName = tableName;
        }
        public SeveredKafkaSource setServers(String servers){
            return  new SeveredKafkaSource(this.topic,servers,this.tableName);
        }
    }
    public static class SeveredKafkaSource{
        private  String topic ;
        private  String servers;
        private  String tableName ;
        public SeveredKafkaSource(String topic, String servers,String tableName) {
            this.topic = topic;
            this.servers = servers;
            this.tableName = tableName;
        }
        public GroupidKafkaSource setGroupid(String groupid){
            return  new GroupidKafkaSource(this.topic,this.servers,groupid,this.tableName);
        }
    }

    public static class GroupidKafkaSource{
        private  LinkedHashMap<String ,String> property = new LinkedHashMap<>();
        private HashSet<String> configSet ;
        private  String tableName ;

        {
            String[] arr = new String[]{
                    "connector",
                    "properties.bootstrap.servers",
                    "scan.startup.mode",
                    "format"
            };
            List<String> list = Arrays.asList(arr);
            this.configSet = new HashSet<>(list);
        }

        public GroupidKafkaSource(String topic, String servers, String groupid,String tableName) {
            this.tableName = tableName;
            property.put("topic",topic);
            property.put("properties.bootstrap.servers",servers);
            property.put("properties.group.id",groupid);
        }

        //TODO 方法待测试
        public GroupidKafkaSource setProperty(String configName,String configValue) throws Exception {
            if ( !configSet.contains(configName)){
                throw  new Exception("不包含此配置名");
            }
            this.property.put(configName,configValue);
            return  this;
        }

        public PropertyKafkaSource build(){
            return  new PropertyKafkaSource(this.property,this.tableName);
        }

    }
    public static class PropertyKafkaSource{

        private  String tableName ;
        private  LinkedHashMap<String ,String> propertyMap ;
        private  LinkedHashMap<String ,String> columeMap = new LinkedHashMap<>();
        private  LinkedHashMap<String ,String> systemColumnMap = new LinkedHashMap<>();


        public PropertyKafkaSource(LinkedHashMap<String, String> property,String tableName) {
            this.propertyMap = property;
            this.tableName = tableName;
            setDefaulProperty();
        }

        //设置一些配置默认值
        private void setDefaulProperty(){
            propertyMap.put("connector","kafka");
            propertyMap.put("scan.startup.mode","latest-offset");
            propertyMap.put("format","json");
        }


        /***
         *  `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',
         *   `partition` BIGINT METADATA VIRTUAL,
         *   `offset` BIGINT METADATA VIRTUAL,
         *
         *
         *     -- 定义列，例如事件时间戳和其他字段
         *   user_id STRING,
         *   item_id STRING,
         *   behavior STRING,
         *   timestamp_ms BIGINT,
         *   -- 事件时间字段，使用 'timestamp_ms' 列作为事件时间戳
         *   event_time AS TO_TIMESTAMP(FROM_UNIXTIME(timestamp_ms / 1000)),
         *   -- 定义 watermark，事件时间戳需要减去的延迟时间，这里假设延迟是5秒
         *   WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
         */

        //TODO 待把类型设置成枚举类
        public  PropertyKafkaSource setColumn(String columnName, String columnType){
            this.columeMap.put(columnName,columnType);
            return this;
        }

        //TODO 待完成
        public  PropertyKafkaSource setSystemColumn( String columnName, String columnType ){
            this.systemColumnMap.put(columnName,columnType);
            return  this;
        }

        public  String getConnectSql(){
            String sqlHeader =  " CREATE TABLE " +  this.tableName + " ( \n" ;
            int columeFlag = 0 ;
            Set<String> columeEntries = columeMap.keySet();
            Set<String>  systemEntries = systemColumnMap.keySet();

            //添加字段,注意最后一行不应有,
            for (String columeName : columeEntries ) {
                columeFlag += 1;
                if(columeFlag < columeEntries.size() ){
                    sqlHeader +=   "`" + columeName + "` " + columeMap.get(columeName) + ", \n";
                }else{
                    //判断是否有system字段,有则末尾加,
                    if(systemEntries.size() ==0 ){
                        sqlHeader +=   "`" + columeName + "` " + columeMap.get(columeName) + " \n";
                    }else {
                        sqlHeader +=   "`" + columeName + "` " + columeMap.get(columeName) + " , \n";
                    }

                }
            }

            //TODO 这里待改良,系统字段是固定类型,除非指定事件时间,不然应该有个映射表
            int systemFlag = 0 ;
            for (String colume : systemEntries) {
                systemFlag +=1 ;
                if (systemFlag<systemEntries.size()){
                    sqlHeader +=  "`" + colume + "` " + systemColumnMap.get(colume) + "  METADATA VIRTUAL , \n";
                }else{
                    sqlHeader +=  "`" + colume + "` " + systemColumnMap.get(colume) + "  METADATA VIRTUAL \n";
                }
            }

            String sqlEnd = ")WITH (\n";
            int properFlag = 0 ;
            Set<String> properEntries = propertyMap.keySet();

            for (String proper : properEntries) {
                properFlag += 1;
                if( properFlag < properEntries.size() ){
                    sqlEnd += "'" + proper + "' = '"  + propertyMap.get(proper) + "' , \n";
                }else{
                    sqlEnd += "'" + proper + "' = '"  + propertyMap.get(proper) + "'  \n" + ")";
                }

            }
            return sqlHeader + sqlEnd ;
        }
    }



}





