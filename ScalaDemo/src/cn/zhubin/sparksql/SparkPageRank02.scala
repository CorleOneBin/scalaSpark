package cn.zhubin.sparksql

import org.apache.spark.{SparkConf, SparkContext}

object SparkPageRank02 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("").setMaster("local")
    val sc = new SparkContext(conf)

    val iters = 20
    val lines = sc.textFile("page.txt")

    val links = lines.map(s =>{
      val parts = s.split("\\s+")
      (parts(0),parts(1))
    }).distinct.groupByKey.cache()

    var ranks = links.mapValues(v => 1.0)

    for(i <- 0 until iters){
        val contribs =  links.join(ranks).values.flatMap(x => {
          val size = x._1.size
          x._1.map(url=>(url,x._2/size))
        })
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
      ranks.checkpoint()
    }

    ranks.foreach(x => {print(x._1)})

  }
}
