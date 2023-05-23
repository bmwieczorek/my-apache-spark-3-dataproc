package com.bartek.spark

import com.bartek.spark.MyEncoders.{MyIntEncoder, MyStringEncoder}


object MyScalaCollection {
  def main(args: Array[String]): Unit = {
    val mySpark = new MySpark
    import mySpark.myImplicits._

    val myStringCollection = new MyCollection[String](Seq("a", "b", "c"))
    println(myStringCollection)
    println(myStringCollection.map(s => s.toUpperCase()))

    System.out.println("=========")

    val myIntCollection = new MyCollection[Int](Seq(1, 2, 3))
    println(myIntCollection)
    println(myIntCollection.map(i => i * i))
  }
}

class MyCollection[T](data: Seq[T]) {

  def map[R: MyEncoder](fun: T => R): MyCollection[R] = { // Accessing an Implicit Parameter Introduced by a Context Bound https://stackoverflow.com/questions/3855595/what-is-the-scala-identifier-implicitly
    val newData: Seq[R] = data.map(t => {
      val newElement = fun(t)
      val myEncoder = implicitly[MyEncoder[R]]
      myEncoder.encode(newElement)
    })
    new MyCollection[R](newData)
  }

  // the above is the same as below
//  def map[R](fun: T => R)(implicit myEncoder: MyEncoder[R]): MyCollection[R] = {
//    val newData: Seq[R] = data.map(t => {
//      val newElement = fun(t)
//      myEncoder.encode(newElement)
//    })
//    new MyCollection[R](newData)
//  }

  override def toString: String = "MyCollection[" + data.mkString(",") + "]"
}

trait MyEncoder[R] {
  def encode[T](t: T): R = {
    println("encoding " + t)
    t.asInstanceOf[R]
  }
}

class MySpark {
  object myImplicits {
    implicit val myStringEncoder: MyStringEncoder = MyEncoders.myStringEncoder()
    implicit val myIntEncoder: MyIntEncoder = MyEncoders.myIntEncoder()
  }
}

object MyEncoders {
  def myStringEncoder(): MyStringEncoder = new MyStringEncoder

  def myIntEncoder(): MyIntEncoder = new MyIntEncoder

  class MyStringEncoder extends MyEncoder[String] {
    override def encode[T](t: T): String = {
      print("Using String encoder for ")
      super.encode(t)
    }
  }

  class MyIntEncoder extends MyEncoder[Int] {
    override def encode[T](t: T): Int = {
      print("Using Int encoder for ")
      super.encode(t)
    }
  }
}
