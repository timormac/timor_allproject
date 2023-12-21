package junit.test;

import junit.java.A1_Calculator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @Title: A1_CalculatorTest
 * @Package: junit.test
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/21 08:48
 * @Version:1.0
 */
public class A1_CalculatorTest {

    private A1_Calculator calculator;

    @Before
    public void setUp(){  this.calculator = new A1_Calculator(); }

    @Test
    public void addTest(){
        int result = calculator.add(3, 5);
        Assert.assertEquals( "3+5=8",2,result );
    }

    @Test
    public void substractTest(){
        int result = calculator.substract(3, 5);
        Assert.assertEquals( "3-5=-2",-2,result );
    }

}
