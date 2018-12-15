package cn.zhubin.test

import scala.io.Source

object FileDemo {
  def main(args:Array[String]):Unit = {
    val s = Source.fromFile("D:/hello.txt","utf-8")
    val lines = s.getLines()
    for(line <- lines){
      println(line)
    }
    val s1 = Source.fromFile("D:/hello.txt","utf-8").mkString
    val it = s1.split("\\s+")
    for (i <- it){
      println(i)
    }
  }
}
