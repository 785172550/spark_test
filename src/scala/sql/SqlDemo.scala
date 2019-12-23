package scala.sql

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

class SqlDemo {
  lazy val spark: SparkSession = Utils.getSession("demo")

  def main(args: Array[String]): Unit = {


  }

  def display(df: DataFrame): Unit = {

    df.select("col1", "col2").show(10)
    df.select(df("col1").as("another"))

    df.first()
    df.take(10) // Array[Row]
    df.head(10) // Array[Row]
    df.takeAsList(10) // List[Row]
    df.limit(10).show() // no action

  }

  def conditionSearch(df: DataFrame): Unit = {
    df.where("age > 21").show
    df.filter("age > 21").show

    df.where(df("age") > 21).show

    df.where("age = 21")
    df.where(df("age ") === 21)
    df.where(df("age ") =!= 21)
    //    val ds = spark.createDataset(df.rdd)

  }

  def aggrate(df: DataFrame): Unit = {

    df.groupBy("province").count().show

    df.groupBy("province", "city")
      .count()
      .withColumnRenamed(existingName = "count", "num")
      .orderBy(df())

    // 按年统计注册用户的最高积分和 平均积分
    df.groupBy(year(df("add_time")))
      .agg(
        max(df("total_mark").as("max_mark")),
        avg(df("total_mark").as("avg_mark"))
      )

    df.groupBy(year(df("add_time")))
      .agg("total_mark" -> "max", "total_mark" -> "avg").show

    //    df.select(month("2018-03-03 20:30:34")) // 3
    //    dayofyear("1122")
  }

  def jsonTransform(df: DataFrame, ds: Dataset[_]): Unit = {
    val schema = new StructType()
      .add("user",
        new StructType()
          .add("uid", StringType)
          .add("name", StringType)
          .add("add_time", StringType)
          .add("total_mark", IntegerType)
      )

    val ds: Dataset[_] = spark.read.schema(schema).json("hdfs://ns/test/logs/user/2019/10/11/*")

    // business code ...

    ds.select().write
      .mode(saveMode = SaveMode.Overwrite)
      .save(s"hdfs://tmp${System.currentTimeMillis()}.parquet")

    spark.close()
  }
}
