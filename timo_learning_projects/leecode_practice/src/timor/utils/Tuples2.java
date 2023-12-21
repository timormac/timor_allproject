package timor.utils;

/**
 * @Author Timor
 * @Date 2023/12/21 12:24
 * @Version 1.0
 */
public class Tuples2<T1,T2> {

    private T1 element1;
    private T2 element2;

    public Tuples2(T1 element1, T2 element2) {
        this.element1 = element1;
        this.element2 = element2;
    }


    public T1 f1(){return this.element1;}
    public T2 f2(){return this.element2;}

    public T1 getElement1() {
        return element1;
    }

    public void setElement1(T1 element1) {
        this.element1 = element1;
    }

    public T2 getElement2() {
        return element2;
    }

    public void setElement2(T2 element2) {
        this.element2 = element2;
    }
}
