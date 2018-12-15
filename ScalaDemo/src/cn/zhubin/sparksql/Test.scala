package cn.zhubin.sparksql

import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("").setMaster("local")
    val sc = new SparkContext(conf)

    val arr = Array(1)
    val arrRDD =  sc.parallelize(arr)
    val shareRDD = arrRDD.map(x => {
      println("==================================")
      x*2})
    //shareRDD.cache()

    val arrRDD1 =  shareRDD.map(_ * 2)
    val list = arrRDD1.collect()

    val arrRDD2 = shareRDD.map(_ * 3)
    val list2 = arrRDD2.collect()


  }
}
