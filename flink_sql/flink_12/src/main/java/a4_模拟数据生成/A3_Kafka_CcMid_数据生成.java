package a4_模拟数据生成;

import a4_模拟数据生成.dao.CcMidDelayData;
import tools.kafka.Kafka_DatMockProduce;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

/**
 * @Title: A3_Kafka_CcMid_数据生成
 * @Package: a4_模拟数据生成
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/23 11:28
 * @Version:1.0
 */
public class A3_Kafka_CcMid_数据生成 {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String topic = "stream_mot_stream_account_break_ths_cc_mid";
        //String server = "10.84.187.61:9097,10.84.187.62:9097,10.84.187.63:9097";

        String server =  "localhost:9092";
        Kafka_DatMockProduce.KafkaSender kafkaProducer = Kafka_DatMockProduce.getKafkaProducer(server, topic);


        ArrayList<CcMidDelayData> list = new ArrayList<CcMidDelayData>();

        list.add( new CcMidDelayData("20240323","18018981934",3) );
        list.add( new CcMidDelayData("20240323","18018981934",3) );
        list.add( new CcMidDelayData("20240323","18018981934",3) );

        kafkaProducer.sendDelayData(list);

    }
}
