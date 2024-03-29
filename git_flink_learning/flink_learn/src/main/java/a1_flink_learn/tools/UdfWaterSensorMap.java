package a1_flink_learn.tools;

import a1_flink_learn.dao.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Title: UdfWaterSensorMap
 * @Package: com.timor.flink.learning.tools
 * @Description:
 * @Author: XXX
 * @Date: 2023/6/27 12:26
 * @Version:1.0
 */
public class UdfWaterSensorMap implements MapFunction<String, WaterSensor> {
    @Override
    public WaterSensor map(String value) throws Exception {
        String[] split = value.split(",");
        return new WaterSensor(split[0],Long.valueOf(split[1]),Integer.valueOf(split[2] ));
    }
}
