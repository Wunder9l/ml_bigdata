package tf_idf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object SparkTfIdf {
  def main(args: Array[String]): Unit = {
    val TOP_N_WORDS = 100

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("SparkTfIdf")
      .getOrCreate()

    val prefix = "/Users/wunder9l/projects/mail_ru/made/3semestr/bigdata/homeworks/4th/SparkTfIdf/"
    val filename = prefix ++ "data/tripadvisor_hotel_reviews.csv"
    println(filename)
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filename)

    val transformed = df
      .select(
        lower(
          regexp_replace(
            regexp_replace(col("Review"), "(\\W+)", " "),
            "(\\d+)",
            " "
          )
        ).as("Value"),
        monotonically_increasing_id().as("DocumentID")
      )
      .cache()
    val totalDocuments = transformed.count().toDouble
    println(s"Total documents ${totalDocuments}")

    val flatten = transformed
      .select(
        col("DocumentID"),
        explode(split(col("Value"), " ")).as("Word")
      )
      .cache()
    flatten.show(200)

    val mostPopularWords = flatten.distinct()
      .groupBy("Word").count()
      .orderBy(desc("count"))
      .where(length(col("Word")) > 1)
      .withColumn("WordLength", length(col("Word")))
      .limit(TOP_N_WORDS)
      .withColumn("Idf", log(lit(totalDocuments)/col("count")))

    val docWindow = Window.partitionBy(col("DocumentID"))
    val termFrequency = flatten
      .groupBy("DocumentID", "Word")
      .agg(count("*").as("WordCount"))
      .withColumn("WordsInDoc", sum(col("WordCount")).over(docWindow))
      .withColumn("Tf", col("WordCount")/col("WordsInDoc"))

    val tfIdf = termFrequency
      .join(mostPopularWords, Seq("Word"))
      .withColumn("TfIdf", col("Tf") * col("Idf"))

    val pivoted = tfIdf.groupBy("DocumentID").pivot("Word")
      .avg("TfIdf")
      .na.fill(0.0)
    pivoted.show()
  }
}
