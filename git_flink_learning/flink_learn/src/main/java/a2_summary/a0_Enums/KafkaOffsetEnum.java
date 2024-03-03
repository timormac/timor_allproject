package a2_summary.a0_Enums;

/**
 * @Author Timor
 * @Date 2024/2/28 16:34
 * @Version 1.0
 */
public enum KafkaOffsetEnum {

    LATEST("latest-offset","最新事件消费"),
    EARLIEST("earliest-offset","从头消费"),
    EXSISTS("group-offsets","按已有的offset消费"),
    TIMESTAMP("timestamp","从用户为每个 partition 指定的时间戳开始"),
    SPECIFY("specific-offsets","从用户为每个 partition 指定的偏移量开始");

    private String format;
    private String msg;

    KafkaOffsetEnum(String format, String msg) {
        this.format = format;
        this.msg = msg;
    }

    public String getFormat() {
        return format;
    }

    public String getMsg() {
        return msg;
    }

}
