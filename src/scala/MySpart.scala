package scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MySpart {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mySpark")
    //setMaster("local") 本机的spark就用local，远端的就写ip
    //如果是打成jar包运行则需要去掉 setMaster("local")因为在参数中会指定。
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
    val input = sc.textFile("./src/resources/students.csv")
    println("partitions: " + input.getNumPartitions)

    val students = input.zipWithIndex.filter(_._2 > 2).keys // first row is title, dont need
    showAll(students)
    showPersonsCount(students)
    showTopPersonbyColumn(students, 19) // grade is in 19 columns
    println("end ------------------")
  }

  def showAll(rdd: RDD[String]): Unit = {
    println("all students data: ")
    rdd.foreach(println)
  }

  def showPersonsCount(rdd: RDD[String]): Unit = {
    println("total rows: " + rdd.count())
  }

  def showTopPersonbyColumn(rdd: RDD[String], column: Int): Unit = {
    val keyValues = rdd.map(line => {
      val fields = line.split(",")
      val name = fields(0)
      val email = fields(1)
      var grade = 0.0
      grade = fields(column).filter(g => Character.isDigit(g)).toDouble // filter bad data and change to double type
      (name, email, grade)
    })

    val res = keyValues.sortBy(x => x._3, false, 1)
    val topGrade = res.first()._3
    res.filter(s => s._3 == topGrade).foreach(println) // find all hightest grade students
  }
}
