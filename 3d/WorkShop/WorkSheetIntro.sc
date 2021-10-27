import au.com.bytecode.opencsv.CSVReader
import breeze.linalg.{DenseMatrix, DenseVector}
import linear_regression._
//import scalaglm.Lm

import java.io.FileReader
import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.Random
//import linear_regression.CSVReader
def parseDatasetLine(cols:Array[String]): Array[Float] = {
  val SEX_TO_FLOAT = HashMap("female"-> -1.0f, "male"-> 1.0f)
  val SMOKER_TO_FLOAT = HashMap("yes"-> -1.0f, "no"-> 1.0f)
  val REGION_TO_FLOAT = HashMap(
    "southeast"-> -1.0f, "southwest"-> -0.5f,
    "northeast"-> 0.5f, "northwest"-> 1.0f,
  )

  return Array(
    cols(0).toFloat,
    SEX_TO_FLOAT(cols(1)),
    cols(2).toFloat,
    cols(3).toFloat,
    SMOKER_TO_FLOAT(cols(4)),
    REGION_TO_FLOAT(cols(5)),
    cols(6).toFloat,
  )
}

def trainTestSplit(dataset: Array[Array[Float]], trainPart:Float): (Array[Array[Float]], Array[Array[Float]]) = {
  val train = new ArrayBuffer[Array[Float]](0)
  val test = new ArrayBuffer[Array[Float]](0)
  for (line <- dataset){
    if (Random.nextFloat() < trainPart) {
      train += line
    } else {
      test += line
    }
  }
  return (train.toArray, test.toArray)
}

def toFeaturesAndTarget(dataset: Array[Array[Float]]): (DenseMatrix[Float], DenseVector[Float]) = {
  val matrix = DenseMatrix.create(dataset(0).length, dataset.length, dataset.flatMap(_.toList)).t
  val X = matrix(::, 0 to matrix.cols - 2)
  val y = matrix(::, matrix.cols - 1)
  return (X, y)
}

val filename = "/Users/wunder9l/projects/mail_ru/made/3semestr/bigdata/homeworks/3d/WorkShop/data/insurance.csv"
val input = new FileReader(filename)
//input.read
//var mat = CSVReader.read(input, skipLines=1)
//mat = mat.takeWhile(line => line.length != 0 && line.head.nonEmpty) // empty lines at the end
//input.close()

val reader = new CSVReader(input, ',', '"'
  , 1)
val lines = reader.readAll()
val datasetRaw = new ArrayBuffer[Array[Float]](0)
lines.forEach(x => {
  datasetRaw += parseDatasetLine(x)
})
val dataset = datasetRaw.toArray
print(dataset.length)
//val matrix = DenseMatrix.create(dataset(0).length, dataset.length, dataset.flatMap(_.toList)).t
//print(matrix.cols, matrix.rows)
//print(matrix.cols - 1)
//matrix(::, matrix.cols - 1)
//matrix(::, 0 to matrix.cols - 2)

//val X = matrix(::, 0 to 5)
//val y = matrix(::, matrix.cols)
val trainTest = trainTestSplit(dataset, 0.5f)
val res = toFeaturesAndTarget(trainTest._1)
val X = res._1
val y = res._2
//val y = matrix(::, 6)
print(X.cols, X.rows)
print(y)
//print(matrix())

//val linesAsArray = lines.forEach(parseDatasetLine)
//linesAsArray.map(parseDatasetLine)
//val source = scala.io.Source.fromFile(filename)
//val data = source.getLines.map(_.split(",")).toArray
//source.close