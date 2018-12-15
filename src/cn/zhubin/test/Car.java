package cn.zhubin.test;


import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

public class Car {
    private int size;
    private String name;

    public Car(int size, String name) {
        this.size = size;
        this.name = name;
    }


    public void ioTest() {
        try {
            BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(""));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
         }
    }

    public static void main(String[] args) {
        System.out.print("123");
    }

}

class SmallCar extends Car {

    public SmallCar(int size, String name) {
        super(size, name);
    }


}
