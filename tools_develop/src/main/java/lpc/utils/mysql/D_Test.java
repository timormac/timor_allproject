package lpc.utils.mysql;

import lpc.utils.mysql.dao.Mock_Cupon;

import java.util.Random;

/**
 * @Author Timor
 * @Date 2024/2/24 20:34
 * @Version 1.0
 */
public class D_Test {
    public static void main(String[] args) throws InterruptedException {

        for (int i = 0; i < 2; i++) {

            Mock_Cupon mock_cupon = new Mock_Cupon();
            mock_cupon.mock("1");
            System.out.println(mock_cupon);

        }


    }
}
