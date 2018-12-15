package cn.zhubin.test

object Calculate {
  def main(args: Array[String]): Unit = {
    val zhiShu = readInt()
    var total = 2
    for (_ <- 0 until zhiShu) {
      total *= 2
    }

    print(total)
  }
}