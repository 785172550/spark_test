package scala

import org.apache.spark.sql.functions.max
import org.apache.spark.sql.{DataFrame, SparkSession}

object TopGrade {

  case class Student(name: String, score: Double)

  def parseLine(line: String): Student = {
    val fields = line.split(",")
    var grade = 0.0
    grade = fields(1).filter(g => Character.isDigit(g)).toDouble // filter bad data and change to double type
    val student: Student = Student(fields(0), grade)
    student
  }

  def main(args: Array[String]) = {

    val spark = SparkSession.builder()
      .appName("topGrade")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // only on windows
      .getOrCreate()

    //    val lines = spark.sparkContext.textFile("./src/resources/students.csv")
    var data = Array("a,100", "b,100", "c,98", "d,78")
    val lines = spark.sparkContext.parallelize(data)
//    val students = lines.zipWithIndex.filter(_._2 > 2).keys // first row is title, dont need

    import spark.implicits._
    val df = lines.map(parseLine).toDF()
    val topGrade = df.agg(max("score")).collect()

    topGrade.foreach(println)




    //     var data = Array( "a,100","b,100","c,98","d,78")
    //     val lines = sc.parallelize(data)
    //     df.foreach(item => println(item.getAs[String]("name")))
    //     showTopGradeStudent(df)
    //     ds.foreach(item => println(item.name)

    // -----------
    //     val stdDS = lines.map(parseLine).toDS().cache()
  }

  def showOut(df: DataFrame) = {
    //       schemaStudent.printSchema()
    //       schemaStudent.creatOrReplaceTempView()
  }
}
