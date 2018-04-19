import com.haiyisoft.bds
import com.haiyisoft.bds.api.data.DataProcessorInter
import com.haiyisoft.bds.api.param.WithParam
import com.haiyisoft.bds.plugins.action.ShowDFAction
import com.haiyisoft.bds.plugins.action.ml.clustering.KMeansClustering
import com.haiyisoft.bds.plugins.loader.IRISLoader
import com.haiyisoft.bds.plugins.process.math.Calculator
import com.haiyisoft.bds.plugins.process.ml.feature.{ChiSqSelector, StringIndex, VectorAssembler}
import com.haiyisoft.bds.plugins.process.udf.{DickFilter, DurationCompute}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


/**
  * Created by XingxueDU on 2018/3/22.
  */
object main {
  def main(args: Array[String]) {
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("Spark2Test")
      .config("spark.sql.warehouse.dir","file:///F:/Travail/workspace").
      config("spark.serializer","org.apache.spark.serializer.KryoSerializer").
      getOrCreate()
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val flow = bds.flow.Builder.newBuilder()

    val disc = new DickFilter()
     println(disc.isInstanceOf[DataProcessorInter])
    printParam(disc)
    val cs = new DurationCompute()
    printParam(cs)
    ////////////////////////////////////
    val loader = new IRISLoader
    flow.appendDataLoader(loader,"L")
    printParam(loader)
    /////////////////////////////////////
    val action = new ShowDFAction
    flow.appendAction(action,"A","T")
    printParam(action)
    ////////////////////////////////
    val VectorGenerator = new VectorAssembler
    printParam(VectorGenerator)


    VectorGenerator.addParam(VectorGenerator.INPUT_COL_KEY,"x1@x2@x3@x4")
    VectorGenerator.setOutputCol("features")
    flow.appendProcessor(VectorGenerator,"T1","L")
    //////////////////////////////////

    val LabelGenerator = new StringIndex

    printParam(LabelGenerator)
    LabelGenerator.setInputCols("labelString")
      .setOutputCol("label")
      .addParam("handleInvalid","error")
    flow.appendProcessor(LabelGenerator,"T2","T1")
    ////////////////////////////////////
    val chi = new ChiSqSelector
    printParam(chi)
    chi.setSelectorType("percentile")
      .setPercentile(0.5)
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setOutputCol("output_chi")
    flow.appendProcessor(chi,"T3","T2")
    ////////////////////////////////////
    val op = new Calculator()
    printParam(op)
    op.setOperator("加法(sum)")
      .setInputCols("x1","x2","x3","x4")
      .setOutputCol("sum")
    flow.appendProcessor(op,"T4","T3")

    ///////////////////////////////
    val trans = new KMeansClustering

    printParam(trans)

    trans.setFeaturesCol("features")
      .setOutputCol("out")
      .setK(3)
      .setSavePath("DO NOT SAVE")
      .setSeed(123L)
    flow.appendProcessor(trans,"T","T4")

    val res = flow.result()
    println()

    println(res.getFlowPlan)
    println()

    res.run()
  }

  def printParam(s:WithParam): Unit ={
    println("\n")
    println("="*20)
    println(s.getClass.getName)
    s.getNecessaryParamList.foreach{k=>
      println()
      println(k.getName)
      println(k.getDescription)
      println(k.checkJsFunction)
    }
  }
}
