package cn.zhubin.test


object  NilTest{
    def main(args:Array[String]):Unit={
      val a = 1::2::3::Nil
      val temp = a.length
      for (i <- 0 until a.length){
        print(a(i))
      }

    }
}