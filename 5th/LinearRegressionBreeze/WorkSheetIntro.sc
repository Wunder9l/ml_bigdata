import au.com.bytecode.opencsv.{CSVReader, CSVWriter}
import breeze.linalg.{DenseMatrix, DenseVector}
import linear_regression._
import scalaglm.Lm

import java.io.{FileReader, FileWriter}
import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
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

val prefix = "/Users/wunder9l/projects/mail_ru/made/3semestr/bigdata/homeworks/5th/LinearRegressionBreeze/"
val filename = prefix ++ "data/insurance.csv"
val input = new FileReader(filename)
//input.read
//var mat = CSVReader.read(input, skipLines=1)
//mat = mat.takeWhile(line => line.length != 0 && line.head.nonEmpty) // empty lines at the end
//input.close()

val reader = new CSVReader(input, ',', '"'
  , 1)
val lines = reader.readAll()
val datasetRaw = new ArrayBuffer[Array[Double]](0)
lines.forEach(x => {
  datasetRaw += parseDatasetLine(x)
})
val dataset = datasetRaw.toArray
print(dataset.length)
val trainTest = trainTestSplit(dataset, 0.8f)
val res = toFeaturesAndTarget(trainTest._1)
val X = res._1
val y = res._2
val COLUMN_NAMES = List("Age", "Sex", "Bmi", "Children", "Smoker", "Region")
val lm = Lm(y,X,COLUMN_NAMES)

lm.coefficients
// res8: DenseVector[Double] = DenseVector(2.0512916253124427, 1.0916855593263126, 0.46491882794443623, -0.297043848299348)
lm.se
// res9: DenseVector[Double] = DenseVector(0.35705191443729006, 0.23966313179996454, 0.14892637571877063, 0.10241655795956897)
lm.fitted
// res10: DenseVector[Double] = DenseVector(3.0894076713644543, 1.9385602250102312, 3.356127712794905, -0.04663797525486818, 6.6929612911351954, 3.3373574224717277, 0.455217446672022, 4.011336957924669, 3.2713174724238496, 3.1029307300411184, 3.425568279922526, 3.8607036512098034, 2.3223093560096215, 3.6463977822753337, 3.1457881612396847, 2.4800875578004744, 2.4288871595256194, 2.9885931273799278, 1.6497904619744066, 4.404804419821866, 2.616264613813348, 3.3832702970673347, 3.387630997755206, 0.5388748548278144, -1.1918948110350958, 1.913334952939593, 1.7049574607475309, 4.060307171443334, 1.3384907563527977, 2.3750278885802008, 3.293882036896411, 1.8546666977181898, 0.4625781855074562, 3.8278705614755255, 0.9300872060390512, 1.0767834188844503, 2.9857809966779287, 2.3144147655972698, 2.4297380666824555, 4.635583549814463, -0.012876178069979982, 2.9549011865491996, 2.1975587302357242, 3.3407628815591606, 2.9682131413889206, 1.046430834643136, 2.569337395725701, 0.5733934156054264, 1.22540080647501, 3.289579575130964, 1.2190987317768829, 4.987348566596525, -1.5692410230707607, 3.5930282313149395, 3.585397915621778, 2.9473938526100785, 1.9266550901018504, 0.7773611261400694, 4.733091550189638, 1.580437577873312, -0.5409244015503756, 0.4458724130086753, -0.6627126811696815, 1.0208776603195262, 1.8774050141137575, 1.7788937258250734, 3.087742663555421, 1.0023081241860914, -0.14949718535941736, 3.9376414650678884, 4.51628648521541, -2.4361969676254622, 2.859502281768578, -0.877141161767276, 1.9726051669012783, 3.4437462000803114, 2.744347949163111, -0.7569525295073385, 2.9802853451212883, 1.4704978909562314, 0.6892655588773626, 2.248204127176616, 3.664620516376165, 3.365287967625667, 2.3856669084310616, 0.83459053378687, 1.6502843317171125, -0.1343785562429889, 2.454550525968803, 3.9428510554482767, 0.5769508727138501, 0.24027116815863714, -0.436393452636187, -0.14694952631753133, 2.1581887480003488, 2.243967437439537, 5.74082404753505, 2.1588072543933157, 4.0341370942416805, 2.8969940121590025)
lm.residuals
// res11: DenseVector[Double] = DenseVector(1.3312925075238065, 1.8326791426704365, -6.702425431121838, 2.3929803422918194, 2.0659601847510762, -1.3398373455623052, -0.7905894851653106, -2.4697554576008893, -5.203637282023104, 5.101441287775581, -0.18494575578587247, -3.159860327325993, 2.0438858150038266, -1.560246396200137, -1.731240815015394, -0.07099802755235318, 0.6394102891271385, -3.653326526417742, 6.270510737859766, 1.033561562906046, -8.71388059288062, 2.0946786547487166, 1.0267442593205693, -0.09961608337617966, -2.216157010561391, 2.255551861794041, 3.491270965667775, -2.339316022950411, -0.6041878425759581, 2.638592162855141, 1.0895808732209082, 1.4411081357722586, -0.4479047579894404, -0.5859776198307118, -0.43137051837653984, -5.590845564818258, 1.1671548909044196, -1.5593620612840389, -1.8410737141464073, 3.2603509543855393, 4.318130723597102, 0.014355154406578752, 1.9742597747601343, 1.3442398746590687, -2.3296554395335427, -1.4120789703795007, -0.8639381723724489, 2.4811013217658444, -1.4102839614670257, 5.046052730467619, -4.229488287822152, 0.17546659649675167, -4.430016064501901, -2.228798485444003, 2.5170126856288215, -1.5326327523143495, -0.20650333679454547, 2.7108981665804452, 6.78274239964219, 0.9005194098625118, -1.6157675401843372, 5.709740327967932, 0.3828346936734154, 0.16987134853287422, 1.8058068660053481, -2.187894481524763, -1.2872369259598193, -0.5574622690836096, 0.5821615626641882, -1.238585006047546, -0.2827750380537646, 0.9535259885176857, -3.3911902507928864, 1.4208554623346283, -4.995744003397338, -0.14460723821917876, -0.9583931324619024, 4.627378802405576, 1.9142998073650705, -0.29934302694400294, 2.9268483279134374, -1.9760289830107252, -0.03250830655385961, 3.593333821781094, -2.9548806904628164, -0.3595131071429356, -1.019909799713536, -0.9835340973326732, -3.081953556443965, 1.7040681755361828, 0.6009498465906795, 3.676275875765741, -2.5210554064464046, -6.662910838610749, 2.682149044732527, 3.160290841174866, -1.929071221472272, 3.099715949256268, -1.3868117840852743, 1.3554866044712628)
lm.studentised
//lm.plots
// res12: DenseVector[Double] = DenseVector(0.46675566273463387, 0.6485641826470897, -2.3657520852724527, 0.8649180859816955, 0.7558884768321481, -0.4709806228559636, -0.28189259905330727, -0.8692619411487865, -1.8475181620229526, 1.7969591926320354, -0.06548630502966932, -1.1324410399290679, 0.7191879932643894, -0.5506653783803709, -0.609375948967074, -0.02494572635862308, 0.2250373543129752, -1.2861523659419583, 2.1911341928516386, 0.36623624596235804, -3.049420080212019, 0.7331284523578802, 0.36410398295674296, -0.034937748357995894, -0.7967630473301518, 0.7878453874123041, 1.2265579898385868, -0.8407666765901453, -0.21361178413030676, 0.9458808526645652, 0.3854611867635246, 0.5059313136831013, -0.16128625393485962, -0.21127818445747132, -0.15249226637649863, -1.9662713623049708, 0.41139527100768164, -0.5580675971719038, -0.6484472589685344, 1.1823851102211258, 1.5307298112009176, 0.005014379870626755, 0.7003043345121233, 0.47603425121669946, -0.8333881469275211, -0.502019768015078, -0.3048490863941407, 0.8741563821862768, -0.4933932345514199, 1.7727019471805754, -1.4861962211108966, 0.06232934445907323, -1.5973282340071242, -0.7827946902912394, 0.8937070401001317, -0.5469969194274316, -0.07216814849059296, 0.9707290643497133, 2.421740751245414, 0.3154624767032869, -0.5888426867614847, 2.025898195684394, 0.1362986615059813, 0.06026585747147191, 0.6320926892137999, -0.7657735027595202, -0.45408879157046317, -0.19513458350333018, 0.20616634305043677, -0.44125396500271347, -0.10082171563437461, 0.35030450761141685, -1.1865650912254575, 0.5056595282698875, -1.7463637285679998, -0.05094362799918985, -0.3356902842615876, 1.6470328938926495, 0.6936417151262733, -0.10833331107451778, 1.042644523682428, -0.6977863421070488, -0.011545281838995542, 1.2741338364328691, -1.0334789298548097, -0.1266429056006241, -0.3626486286773428, -0.3519820937899213, -1.0882132508573605, 0.5999298552833771, 0.2121067949644124, 1.2969705726754865, -0.891485747677704, -2.3588120024366344, 0.9453794370830678, 1.1028930478591175, -0.6936422062002296, 1.0933432456540364, -0.4885702222010265, 0.48386503311402607)

val testXy = toFeaturesAndTarget(trainTest._2)
val pred = lm.predict(testXy._1)
var output = testXy._1
val actualY = testXy._2.toDenseMatrix.reshape(output.rows, 1)
output = DenseMatrix.horzcat(output, actualY.toDenseMatrix)
output = DenseMatrix.horzcat(output, pred.fitted.toDenseMatrix.reshape(output.rows, 1))
//print(output.rows, output.cols, pred.fitted.toDenseMatrix.reshape.rows, actualY.cols)
//output(::, output.cols) := pred.fitted
print(output.rows, output.cols)
print(output)

val outFilename = prefix ++ "data/results.csv"
val outputFile = new FileWriter(outFilename)
val csvWriter = new CSVWriter(outputFile, ',', CSVWriter.NO_QUOTE_CHARACTER)
csvWriter.writeNext((COLUMN_NAMES ++ List("ActualValue", "PredictedValue")).toArray)
for (i <- 0 until output.rows) {
  println(i, "out from ", output.rows)
//  print(output(i,::).inner.toArray.map(_.toString))
  csvWriter.writeNext(output(i,::).inner.toArray.map(_.toString))
}
csvWriter.close()