package cn.zhubin.sparksql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

case class Student(id:Int,name:String,age:Int)

object RDD2DataFrameReflection {
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //在scala中使用反射方式进行RDD到DataFrame的转换，需要手动导入一个隐式转换
    import sqlContext.implicits._

    val students = sc.textFile("students.txt")
      .map(line => line.split(",")).map(array => Student(array(0).trim.toInt,array(1),array(2).trim.toInt))

    //直接使用RDD的toDF()方法即可转换为DataFrame
    val studentDF = students.toDF()
    studentDF.registerTempTable("students")
    val teenagerDF =  sqlContext.sql("select * from students where age <= 18")
    val teenagerRDD = teenagerDF.rdd

    //没有像java一样按照字典排序
    teenagerDF.map(row => Student(row(0).toString.toInt,row(1).toString,row(2).toString.toInt))

    teenagerRDD.map(row => Student(row(0).toString.toInt,row(1).toString,row(2).toString.toInt))


  }
}
