package tools.kafka.dao;

/**
 * @Title: AbstractDelayData
 * @Package: tools.kafka
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/23 10:38
 * @Version:1.0
 */
public abstract class AbstractDelayData implements MockData {

    private int delaySeconds;

    public AbstractDelayData(int delaySeconds) {
        this.delaySeconds = delaySeconds;
    }

    public abstract void excuteUpdate();
    public int getDelaySeconds(){
        return this.delaySeconds;
    }

}
