package cn.zhubin.test

object UnapplyTest {
  def apply(n: Int, d: Int) = new UnapplyTest(n, d)

  def unapply(u: UnapplyTest) = Some(u.n, u.d)

  def main(args: Array[String]): Unit = {
    val a = UnapplyTest(1,2)
    val UnapplyTest(c,d) = a
  }

}

class UnapplyTest(val n: Int, val d: Int) {

}
