package lpc.utils.mysql.dao;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Map;

/**
 * @Author Timor
 * @Date 2024/2/24 18:55
 * @Version 1.0
 */
 public abstract class AbsctractMockDao  implements Dao,Mocked{

    @Override
    public Dao getInstance(Map<String, Object> map) throws IllegalAccessException {
        Field[] fields = this.getClass().getDeclaredFields();

        for (Field field : fields) {
            field.set(this , map.get(field.getName())  );
        }

        return this;
    }


}
