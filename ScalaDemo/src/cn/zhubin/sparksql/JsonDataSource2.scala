package cn.zhubin.sparksql

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object JsonDataSource2{
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("JsonDataSource2").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val studentScoreDF = sqlContext.read.json("ScalaDemo\\student.json")
    studentScoreDF.registerTempTable("student_score")
    val goodStudentScoreDF =  sqlContext.sql("select name , score from student_score where score >= 80")
    val goodStudentNames =  goodStudentScoreDF.rdd.map((row:Row) => row(0)).collect

    val studentInfoJsons = Array("{\"name\":\"Yasaka\", \"age\":18}",
      "{\"name\":\"Xuruyun\", \"age\":17}",
      "{\"name\":\"Liangyongqi\", \"age\":19}")
    val studentInfoRDD =  sc.parallelize(studentInfoJsons)
    val studentInfoDF =  sqlContext.read.json(studentInfoRDD)
    studentInfoDF.registerTempTable("student_info")
    var sql = "select name,age from student_info where name in("
    for(i <- 0 until goodStudentNames.length){
      sql+= "'" +goodStudentNames(i) +"'"
      if(i < goodStudentNames.length-1){
        sql+=","
      }
    }
    sql+=")"

    val goodStudentInfoDF = sqlContext.sql(sql)

    val goodStudentRDD =  goodStudentScoreDF.rdd.map(row =>(row.getAs[String]("name"),row.getAs[Long]("score")) )
      .join(goodStudentInfoDF.rdd.map(row => (row.getAs[String]("name"),row.getAs[Long]("age"))))

    val goodStudentRow =  goodStudentRDD.map(info => Row(info._1,info._2._1,info._2._2))
    val structType = StructType(Array(
      StructField("name",StringType,true),
      StructField("score",LongType,true),
      StructField("age",LongType,true)
    ))

    val goodStudentDF =  sqlContext.createDataFrame(goodStudentRow,structType)
    val list =   goodStudentDF.collect()
    goodStudentDF.write.format("json").json("  ")

  }
}