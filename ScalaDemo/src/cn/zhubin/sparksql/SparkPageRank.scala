package cn.zhubin.sparksql

import org.apache.spark.{SparkConf, SparkContext}

object SparkPageRank {
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("").setMaster("local")
    val sc = new SparkContext(conf)

    val iters = 20
    val lines = sc.textFile("page.txt")

    //根据边关系数据生成，邻接表：(1,(2,3,4,5)) (2,(1,5))....
    val links =  lines.map(s => {
      val parts = s.split("\\s+")
      (parts(0),parts(1))
    }).distinct().groupByKey().cache()

    //(1,1.0) (2,2.0)
    var ranks = links.mapValues(v => 1.0)


    for(i <- 1 to iters){
      //(1,(2,3,4,5),1.0)
        val contribs = links.join(ranks).values.flatMap(x => {
          val size = x._1.size            //x._1  :  (2,3,4,5)
          x._1.map(url => (url,x._2/size))       //返回的是一个集合，(2,0.25) (3,0.25)...  所以要用flatmap
        })
        ranks = contribs.reduceByKey(_+_).mapValues(0.15 + 0.85 * _)  //把一个节点的度加起来 后面这个是公式
        ranks.checkpoint()
      }
      ranks.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
    }

}
