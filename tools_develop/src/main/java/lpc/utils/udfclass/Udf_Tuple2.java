package lpc.utils.udfclass;

/**
 * @Title: UdfTuple2
 * @Package: lpc.utils.udfclass
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/21 03:20
 * @Version:1.0
 */
public class Udf_Tuple2<F1,F2> {

    private F1 f1Data;
    private F2 f2Data;

    public Udf_Tuple2(F1 f1Data, F2 f2Data) {
        this.f1Data = f1Data;
        this.f2Data = f2Data;
    }

    public F1 f1(){

        return f1Data;
    }

    public F2 f2(){
        return f2Data;
    }


    @Override
    public String toString() {
        return  "( " + f1Data + " | " + f2Data + " )";
    }
}
