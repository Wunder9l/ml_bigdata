import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val spark = SparkSession.builder()
  // адрес мастера
  .master("local[*]")
  // имя приложения в интерфейсе спарка
  .appName("SparkTfIdf")
  // взять текущий или создать новый
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
transformed.show(20)

val totalDocuments = transformed.count().toDouble
println(s"Total documents ${totalDocuments}")

val flatten = transformed
  .select(
    col("DocumentID"),
    explode(split(col("Value"), " ")).as("Word")
  )
  .cache()
flatten.show(200)

val TOP_N_WORDS = 100
val mostPopularWords = flatten.distinct()
  .groupBy("Word").count()
  .orderBy(desc("count"))
  .where(length(col("Word")) > 1)
  .withColumn("WordLength", length(col("Word")))
  .limit(TOP_N_WORDS)
  .withColumn("Idf", log(lit(totalDocuments)/col("count")))

mostPopularWords.show(200)

val termFrequency = flatten
  .groupBy("DocumentID", "Word")
  .agg(count("*").as("WordCount"))

val docWindow = Window.partitionBy(col("DocumentID"))
val withDocWords = termFrequency
.withColumn("WordsInDoc", sum(col("WordCount")).over(docWindow))
.withColumn("Tf", col("WordCount")/col("WordsInDoc"))

withDocWords.where(col("DocumentID") < 2).show(200)

val tfIdf = withDocWords
.join(mostPopularWords, Seq("Word"))
.withColumn("TfIdf", col("Tf") * col("Idf"))
tfIdf.show()

val pivoted = tfIdf.groupBy("DocumentID").pivot("Word")
  .avg("TfIdf")
  .na.fill(0.0)
pivoted.show()