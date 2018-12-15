package cn.zhubin.test

object PatternTest {
  def main(args:Array[String]):Unit={
    val x = '9'
    x match{
      case '+' => print("++++")
      case '-' => print("----")
      case _ if Character.isDigit(x) => print("is number")
      case _ => print("....")
    }

    val x1:Any = "123"
    x1 match{
      case b:Int => print("is Int")
      case a:String => print("is String")
      case _ => print("is int")
    }
  }
}
