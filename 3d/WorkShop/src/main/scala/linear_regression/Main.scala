package linear_regression

import linear_regression.Utils
import au.com.bytecode.opencsv.CSVReader
import breeze.linalg._

import java.io._
import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object Main {
  def parseDatasetLine(cols: Array[String]): Array[Float] = {
    val SEX_TO_FLOAT = HashMap("female" -> -1.0f, "male" -> 1.0f)
    val SMOKER_TO_FLOAT = HashMap("yes" -> -1.0f, "no" -> 1.0f)
    val REGION_TO_FLOAT = HashMap(
      "southeast" -> -1.0f, "southwest" -> -0.5f,
      "northeast" -> 0.5f, "northwest" -> 1.0f,
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

  def trainTestSplit(dataset: Array[Array[Float]], trainPart: Float): (Array[Array[Float]], Array[Array[Float]]) = {
    val train = new ArrayBuffer[Array[Float]](0)
    val test = new ArrayBuffer[Array[Float]](0)
    for (line <- dataset) {
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

  def main(array: Array[String]): Unit = {

    val filename = "/Users/wunder9l/projects/mail_ru/made/3semestr/bigdata/homeworks/3d/WorkShop/data/insurance.csv"
    val input = new FileReader(filename)
    val reader = new CSVReader(input, ',', '"'
      , 1)
    val lines = reader.readAll()
    input.close()


    val dataset = new ArrayBuffer[Array[Float]](0)
    lines.forEach(x => {
      dataset += parseDatasetLine(x)
    })
    DenseMatrix()
  }
}
