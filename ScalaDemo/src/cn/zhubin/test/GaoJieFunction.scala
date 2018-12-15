package cn.zhubin.test

object GaoJieFunction {

  //可以将函数赋值给一个变量/常量

  def add(a:Int,b:Int):Int={a+b}
  val f = add _

  def multi(n:Int):Int=n*2
  val f1 = multi _

  //匿名函数
  (n:Double) => 3*n

  val f3 = (n:Double) => 3*n


  //参数为函数的函数
  def call(a:Int,b:Int,f1:(Int,Int)=>Int,f2:(Int,Int)=>Int): Int ={
    if (a>0){
      f1(a,b)
    }else{
      f2(a,b)
    }
  }

  //参数为函数，且返回值为函数

  def call1(a:Int,b:Int,f1:(Int,Int)=>Int,f2:(Int,Int)=>Int): (Int)=>Int ={
    var n = 0
    if (a>0){
      n = f1(a,b)
    }else{
      n = f2(a,b)
    }

    def multi(x:Int):Int={x*n}
    multi _

  }

  //传入一个参数，返回一个函数. 克里化写法
  def mulby(factor:Double)=(x:Double)=>{x*factor}







  //控制抽象
  def newThread(block:()=>Unit)={
    new Thread(){
      override def run(): Unit ={
        block
      }
    }
  }


  def sum(list:List[Int]):Int={
    if (list == Nil)
     0
    else
      list.head + sum(list.tail)
  }


  def main(args:Array[String]):Unit={
    sum(List(1,2,3,4,5))
      }

}
