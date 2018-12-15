package cn.zhubin.sparksql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object UDAF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val names = Array("Yasaka","Xuruyun","Wangfei","Xuruyun","Xuruyun")
    val namesRDD = sc.parallelize(names)
    val namesRow = namesRDD.map(name => Row(name))
    val structType = StructType(Array(StructField("name",StringType,true)))
    val namesDF = sqlContext.createDataFrame(namesRow,structType)

    namesDF.registerTempTable("names")

    sqlContext.udf.register("strGroupCount",new StringGroupCount())

    sqlContext.sql("select name,strGroupCount(name) from names group by name")
      .collect().foreach(println)   //注意：这里的foreach不是算子，而是scala的函数。因为collect，已经在driver端了

  }
}
