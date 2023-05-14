package com.bartek.spark;

import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.List;

public class MyJavaCollection {
    public static void main(String[] args) {
        Seq<String> stringSeq = asScalaSeq(Arrays.asList("a", "b", "c"));
        MyCollection<String> myStringCollection = new MyCollection<>(stringSeq);
        System.out.println(myStringCollection);
        System.out.println(myStringCollection.map(String::toUpperCase, MyEncoders.myStringEncoder()));

        System.out.println("=========");

        Seq<Integer> integerSeq = asScalaSeq(Arrays.asList(1, 2, 3));
        MyCollection<Integer> myIntCollection = new MyCollection<>(integerSeq);
        System.out.println(myIntCollection);
        System.out.println(myIntCollection.map(i -> i * i, MyEncoders.myIntEncoder()));
    }

    private static <T> Seq<T> asScalaSeq(List<T> list) {
        return JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
    }
}
