package e_objectprogramming;

/**
 * @Author Timor
 * @Date 2023/12/4 13:36
 * @Version 1.0
 */

//注意,枚举必须有枚举实例，不然会报红
public enum G_Enum {
    SUCCESS(200,"成功"),
    FAIL(100,"失败");

    private int code;
    private String text;

     G_Enum(int code, String text) {
        this.code = code;
        this.text = text;
    }

    //enum关键字，继承了Enum抽象类，直接点看不到，要用control+H才能看到
    public  static G_Enum getByCode (int code) {
        for (G_Enum value : values()) {
            if (value.getCode() == code) return value;
        }
        return null;
    }


    public int getCode() {
        return code;
    }

    public String getText() {
        return text;
    }

}
