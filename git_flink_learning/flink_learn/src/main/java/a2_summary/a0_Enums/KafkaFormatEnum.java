package a2_summary.a0_Enums;

/**
 * @Author Timor
 * @Date 2024/2/28 16:47
 * @Version 1.0
 */
public enum KafkaFormatEnum {

    JSON("json"),
    CSV("csv");


    private  String value;

    KafkaFormatEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
