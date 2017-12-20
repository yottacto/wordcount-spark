package org.apache.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object wordcount {

  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: SparkWordCount <input> <output>")
      System.exit(1)
    }

    val spark = SparkSession
      .builder
      .appName("SparkWordCount")
      .getOrCreate()

    import spark.implicits._
 
    val data = spark.read.text(args(0)).as[String]
    val words = data.flatMap(line => line.split("\\s+"))

    val groupedWords = words.groupByKey(_.toLowerCase)
    val counts = groupedWords.count().sort(desc("count(1)"))

    counts.show()
    
    counts.coalesce(1).write.format("csv").save(args(1))

    spark.stop()
  }

}
