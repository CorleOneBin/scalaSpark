package cn.zhubin.sparksql

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object JsonDataSource {
  def main(ars:Array[String]):Unit={
    val conf = new SparkConf().setAppName("JsonDataSource").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //读取学生的成绩信息
    val studentScoreDF = sqlContext.read.json("ScalaDemo\\student.json")
    studentScoreDF.registerTempTable("student_scores")

    //取出成绩>=80的学生
    val goodStudentScoreDF = sqlContext.sql("select name , score from student_scores where score >= 80")
    //取出成绩>=80的学生的姓名
    val goodStudentnames = goodStudentScoreDF.rdd.map((row:Row) => row(0)).collect

    //创建学生的基本信息
    val studentInfoJsons =  Array("{\"name\":\"Yasaka\", \"age\":18}",
      "{\"name\":\"Xuruyun\", \"age\":17}",
      "{\"name\":\"Liangyongqi\", \"age\":19}")
    val studentInfoRDD = sc.parallelize(studentInfoJsons)
    val studentInfoDf =  sqlContext.read.json(studentInfoRDD)

    studentInfoDf.registerTempTable("student_infos")

    //取出好学生的基本信息
    var sql = "select name,age from student_infos where name in ("
    for(i <- 0 until goodStudentnames.length){
      sql += "'" + goodStudentnames(i) +"'"
      if( i < goodStudentnames.length - 1){
        sql += ","
      }
    }
    sql += ")"
    print(sql)
    val goodStudentInfoDf =  sqlContext.sql(sql)


    //join
    val goodStudentRDD = goodStudentScoreDF.rdd.map(row => (row.getAs[String]("name"),row.getAs[Long]("score")))
      .join(goodStudentInfoDf.rdd.map((row:Row) => (row.getAs[String]("name"), row.getAs[Long]("age"))));


    val goodStudentRow =  goodStudentRDD.map(info => Row(info._1,info._2._1,info._2._1))

    val structType =StructType(Array(
      StructField("name",StringType,true),
      StructField("score",LongType,true),
      StructField("age",LongType,true)
    ))

    val goodStudentDF = sqlContext.createDataFrame(goodStudentRow,structType)

    val list1 =  goodStudentDF.rdd.collect()

    goodStudentDF.write.format("json").save("C:\\Users\\Bin\\Desktop\\studentJson")


  }
}
