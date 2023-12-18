package a3_math_utils;

import com.google.common.math.IntMath;

import java.math.RoundingMode;

/**
 * @Title: A1_Math
 * @Package: a3_math_utils
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/18 17:12
 * @Version:1.0
 */
public class A1_Math {
    public static void main(String[] args) {

        int logBase2 = IntMath.log2(16, RoundingMode.DOWN);


        int squareRoot = IntMath.sqrt(16, RoundingMode.DOWN);


        int power = IntMath.pow(2, 3);


        boolean isPowerOfTwo = IntMath.isPowerOfTwo(16);






    }
}
