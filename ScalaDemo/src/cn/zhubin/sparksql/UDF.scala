package cn.zhubin.sparksql

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object UDF {
  def main(args:Array[String]):Unit={

    def myUDF(name:String):Int = name.length

    val conf = new SparkConf().setAppName("").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val names = Array("Yasaka","zhubin","wangyujuan")
    val namesRDD =  sc.parallelize(names,3)
    val namesRow = namesRDD.map(name => Row(name))
    val structType = StructType(Array(StructField("name",StringType,true)))
    val namesDF =  sqlContext.createDataFrame(namesRow,structType)

    namesDF.registerTempTable("names")

    //注册成为UDF
    //sqlContext.udf.register("strLength",myUDF _)

    sqlContext.sql("select name from names").collect()
      .foreach(println)

  }
}
