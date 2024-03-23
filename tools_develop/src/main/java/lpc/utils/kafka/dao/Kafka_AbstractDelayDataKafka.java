package lpc.utils.kafka.dao;

/**
 * @Title: AbstractDelayData
 * @Package: tools.kafka
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/23 10:38
 * @Version:1.0
 */
public abstract class Kafka_AbstractDelayDataKafka implements Kafka_MockData {

    private int delaySeconds;

    public Kafka_AbstractDelayDataKafka(int delaySeconds) {
        this.delaySeconds = delaySeconds;
    }

    public abstract void excuteUpdate();
    public int getDelaySeconds(){
        return this.delaySeconds;
    }

}
