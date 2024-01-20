package l_advanced;

import java.io.Serializable;

/**
 * @Title: A1_Searilizable
 * @Package: l_advanced
 * @Description:
 * @Author: lpc
 * @Date: 2024/1/15 19:14
 * @Version:1.0
 */
public class A1_Serializable {
    public static void main(String[] args) {

        Student tom = new Student("tom", 18);
        Student peter = new Student("peter", 22);

        Student[] arr = new Student[2];
        arr[1]= tom;
        arr[2]= peter;





    }


}

class Student implements Serializable {
    String name;
    int age;

    public Student(String name, int age) {
        this.name = name;
        this.age = age;
    }

    @Override
    public String toString() {
        return "Student{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }

}
