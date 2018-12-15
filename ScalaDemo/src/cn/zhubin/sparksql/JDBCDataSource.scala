package cn.zhubin.sparksql

import java.sql.DriverManager
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

object JDBCDataSource {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JDBCDataSource").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    var options = new mutable.HashMap[String, String]()
    options.put("url", "jdbc:mysql://node1:3306/test")
    options.put("user", "root")
    options.put("password", "zhubin")
    options.put("dbtable", "student_infos")

    val studentInfosDF = sqlContext.read.format("jdbc").options(options).load()

    options.put("dbtable", "student_scores")
    val studentScoreDF = sqlContext.read.format("jdbc").options(options).load()

    //join操作
    val studentsRDD = studentInfosDF.rdd.map(row => (row.getString(0), row.getInt(1)))
      .join(studentScoreDF.rdd.map(row => (row.getString(0), row.getInt(1))))

    //转换成Row
    val studentsRow = studentsRDD.map(info => Row(info._1, info._2._1, info._2._2))
    //过滤成绩>80的
    val goodStudentsRow = studentsRow.filter(row => row.getInt(2) > 80)

    val structType  = StructType(Array(
      StructField("name",StringType,true),
      StructField("score",IntegerType,true),
      StructField("age",IntegerType,true)
    ))


    val goodStudentDF =  sqlContext.createDataFrame(goodStudentsRow,StructType(structType))
    for(row <- goodStudentDF){
      print(row)
    }

    goodStudentDF.rdd.foreach(row =>{
      val sql ="insert into good_student_infos values(" +"'"+ row.getString(0) +"'" + "," + row.getInt(1) +","
      +row.getInt(2) +")"

      Class.forName("com.mysql.jdbc.Driver")
      val conn = DriverManager.getConnection("jdbc:mysql://node1:3306/student","root","zhubin")
      val stat = conn.createStatement()
      stat.execute(sql)

      stat.close()
      conn.close()

    })
  }
}

