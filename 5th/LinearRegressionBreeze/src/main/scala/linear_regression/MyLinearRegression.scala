package linear_regression

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.stats.regression.leastSquares

class MyLinearRegression(var columns: List[String], X: DenseMatrix[Double], y: DenseVector[Double]) {
  def addFreeCoef(X: DenseMatrix[Double]): DenseMatrix[Double] = {
    val freeCoef = DenseVector.ones[Double](X.rows).toDenseMatrix.t
    return DenseMatrix.horzcat(freeCoef, X)
  }

  var weights = leastSquares(addFreeCoef(X), y)

  def predict(x: DenseMatrix[Double]): DenseVector[Double] = {
    val withCoef = addFreeCoef(x)
    return weights.apply(withCoef)
  }
}
