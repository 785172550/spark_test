package scala

object Test {

  def main(args: Array[String]): Unit = {
//    println("tt")
//    qSort(List(22,4,3,1,66,7,89,99,0,97,887,6,8,77,66,55,22)).foreach(println) //StackOverflowError
  }

  def qSort(arr: List[Int]): List[Int] =
    if (arr.length < 2) arr
    else
      qSort(arr.filter(_ < arr.head)) ++ qSort(arr.filter(_ == arr.head)) ++ qSort(arr.filter(_ > arr.head))
}
