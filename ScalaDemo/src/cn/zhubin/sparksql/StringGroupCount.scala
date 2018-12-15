package cn.zhubin.sparksql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class StringGroupCount extends UserDefinedAggregateFunction{

  //输入的类型
  override def inputSchema: StructType = {
    StructType(Array(StructField("str",StringType,true)))
  }

  //中间的结果类型
  override def bufferSchema: StructType = {
    StructType(Array(StructField("count",IntegerType,true)))
  }

  //最后的结果类型
  override def dataType: DataType = {
    IntegerType
  }

  //一直设为true就好
  override def deterministic: Boolean = {
    true
  }

  //初始化    因为是算数量，所以只需要返回一个值
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
  }

  //中间结果，即每个partition的处理结果,每遇到一个相同就加1
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Int](0)+1
  }

  //最后的结果，各个partition的结果进行相加
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
  }

  //返回的结果
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Int](0)
  }
}
