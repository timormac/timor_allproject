package a1_flink_learn.dao;

/**
 * @Author Timor
 * @Date 2023/12/14 17:42
 * @Version 1.0
 */
public class AcidTableDao {
    public String  id;
    public String  name;

    public AcidTableDao(String id, String name) {
        this.id = id;
        this.name = name;
    }

    public AcidTableDao() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "AcidTableDao{" +
                "id=" + id +
                ", name='" + name + '\'' +
                '}';
    }
}
