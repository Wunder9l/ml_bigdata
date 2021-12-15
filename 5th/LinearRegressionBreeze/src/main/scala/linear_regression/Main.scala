package linear_regression

import au.com.bytecode.opencsv.{CSVReader, CSVWriter}
import breeze.linalg._

import java.io._
import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object Main {
  def parseDatasetLine(cols:Array[String]): Array[Double] = {
    val SEX_TO_DOUBLE = HashMap("female"-> -1.0, "male"-> 1.0)
    val SMOKER_TO_DOUBLE = HashMap("yes"-> -1.0, "no"-> 1.0)
    val REGION_TO_DOUBLE = HashMap(
      "southeast"-> -1.0, "southwest"-> -0.5,
      "northeast"-> 0.5, "northwest"-> 1.0,
    )

    return Array(
      cols(0).toDouble,
      SEX_TO_DOUBLE(cols(1)),
      cols(2).toDouble,
      cols(3).toDouble,
      SMOKER_TO_DOUBLE(cols(4)),
      REGION_TO_DOUBLE(cols(5)),
      cols(6).toDouble,
    )
  }

  def trainTestSplit(dataset: Array[Array[Double]], trainPart:Float): (Array[Array[Double]], Array[Array[Double]]) = {
    val train = new ArrayBuffer[Array[Double]](0)
    val test = new ArrayBuffer[Array[Double]](0)
    for (line <- dataset){
      if (Random.nextFloat() < trainPart) {
        train += line
      } else {
        test += line
      }
    }
    return (train.toArray, test.toArray)
  }

  def toFeaturesAndTarget(dataset: Array[Array[Double]]): (DenseMatrix[Double], DenseVector[Double]) = {
    val matrix = DenseMatrix.create(dataset(0).length, dataset.length, dataset.flatMap(_.toList)).t
    val X = matrix(::, 0 to matrix.cols - 2)
    val y = matrix(::, matrix.cols - 1)
    return (X, y)
  }

  def main(array: Array[String]): Unit = {

    val prefix = "/Users/wunder9l/projects/mail_ru/made/3semestr/bigdata/homeworks/5th/LinearRegressionBreeze/"
    val filename = prefix ++ "data/insurance.csv"
    val input = new FileReader(filename)

    val reader = new CSVReader(input, ',', '"', 1)
    val lines = reader.readAll()
    val datasetRaw = new ArrayBuffer[Array[Double]](0)
    lines.forEach(x => {
      datasetRaw += parseDatasetLine(x)
    })
    val dataset = datasetRaw.toArray

    val trainTest = trainTestSplit(dataset, 0.8f)
    val res = toFeaturesAndTarget(trainTest._1)
    val X = res._1
    val y = res._2

    val COLUMN_NAMES = List("Age", "Sex", "Bmi", "Children", "Smoker", "Region")

    val myLm = new MyLinearRegression(COLUMN_NAMES, X, y)
    val testXy = toFeaturesAndTarget(trainTest._2)
    val pred = myLm.predict(testXy._1).toDenseMatrix.t
    var output = testXy._1
    val actualY = testXy._2.toDenseMatrix.t
    output = DenseMatrix.horzcat(output, actualY)
    output = DenseMatrix.horzcat(output, pred)

    val outFilename = prefix ++ "data/results.csv"
    val outputFile = new FileWriter(outFilename)
    val csvWriter = new CSVWriter(outputFile, ',', CSVWriter.NO_QUOTE_CHARACTER)
    csvWriter.writeNext((COLUMN_NAMES ++ List("ActualValue", "PredictedValue")).toArray)
    for (i <- 0 until output.rows) {
      csvWriter.writeNext(output(i,::).inner.toArray.map(_.toString))
    }
    csvWriter.close()
  }
}
