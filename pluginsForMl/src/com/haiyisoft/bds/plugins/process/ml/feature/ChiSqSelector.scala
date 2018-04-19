package com.haiyisoft.bds.plugins.process.ml.feature

import com.haiyisoft.bds.api.action.ModelTraining
import com.haiyisoft.bds.api.data.Model
import com.haiyisoft.bds.api.data.trans.DataFeaturesInter
import com.haiyisoft.bds.api.param.ParamFromUser
import com.haiyisoft.bds.api.param.col.{HasFeaturesColParam, HasLabelColParam, HasOutputParam}
import org.apache.spark.mllib.feature.{ ChiSqSelectorModel => MLModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.linalg.{Vectors=>OldVectors}
import org.apache.spark.mllib.regression.{LabeledPoint=>OldLabeledPoint}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, Row, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

/**
  * Created by XingxueDU on 2018/3/23.
  */
class ChiSqSelector extends DataFeaturesInter with ModelTraining[ChiSqSelectorModel] with ChiSqSelectorParam{
//  private val training = new MLTraining()
  /**
    * 模型训练
    *
    * @param dataset 训练集
    * @return 生成的模型
    */
  override def fit(dataset: DataFrame): ChiSqSelectorModel = {
//    val model = training.setFeaturesCol(getFeaturesCol)
//      .setLabelCol(getLabelCol)
//      .setNumTopFeatures(getNumTopFeatures)
//      .setOutputCol(getOutputCol)
//      .fit(dataset)
    val input: RDD[OldLabeledPoint] =
      dataset.select(col(getLabelCol).cast(DoubleType), col(getFeaturesCol)).rdd.map {
        case Row(label: Double, features: Vector) =>
          OldLabeledPoint(label, OldVectors.fromML(features))
      }
    val chiSqTestResult = Statistics.chiSqTest(input).zipWithIndex
    val features = getSelectorType match {
      case NUM_TOP_FEATURES_KEY =>
        chiSqTestResult
          .sortBy { case (res, _) => res.pValue }
          .take(getNumTopFeatures)
      case PERCENTILE_KEY =>
        chiSqTestResult
          .sortBy { case (res, _) => res.pValue }
          .take((chiSqTestResult.length * getPercentile).toInt)
      case FPR_KEY =>
        chiSqTestResult
          .filter { case (res, _) => res.pValue < getFpr }
      case FDR_KEY =>
        // This uses the Benjamini-Hochberg procedure.
        // https://en.wikipedia.org/wiki/False_discovery_rate#Benjamini.E2.80.93Hochberg_procedure
        val tempRes = chiSqTestResult
          .sortBy { case (res, _) => res.pValue }
        val maxIndex = tempRes
          .zipWithIndex
          .filter { case ((res, _), index) =>
            res.pValue <= getFdr * (index + 1) / chiSqTestResult.length }
          .map { case (_, index) => index }
          .max
        tempRes.take(maxIndex + 1)
      case FWE_KEY =>
        chiSqTestResult
          .filter { case (res, _) => res.pValue < getFwe / chiSqTestResult.length }
      case errorType =>
        throw new IllegalStateException(s"Unknown ChiSqSelector Type: $errorType")
    }
    val selectedFeatures = features.map { case (_, index) => index }
    val model = new MLModel(selectedFeatures)
    new ChiSqSelectorModel(model)
  }

  /**
    * 保存对象.注意：保存的是结果而不是自身
    *
    * @param stored 被保存对象（dataFrame或者Model）
    */
  override def save(stored: ChiSqSelectorModel): Unit = {
    if(isWantSave)stored.toSave(getSavePath)
  }

  /**
    * 变换的执行接口，从待变换的data转化为变换后的data
    *
    * @param data 待变换的表
    * @return 变换后的表
    */
  override def transform(data: DataFrame): DataFrame = {
    val model = fit(data)
    save(model)
    model.setFeaturesCol(getFeaturesCol)
        .setOutputCol(getOutputCol)
      .transform(data)
  }

  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  override def getNecessaryParamList: Array[ParamFromUser] = {
    val features = featuresColFromUser
    val label = labelColFromUser
    val selectorType = selectorTypeFromUser
    val param = paramForSelectorFromUser
    val out = ParamFromUser.String(OUTPUT_COL_KEY,"请输入结果的输出列名#作为中间流程时请输入结果的输出列名。作为执行流程时请保留默认值",notEmpty = true).addDefaultValue("features")
    Array(features,label,selectorType,param,out)
  }

  override def getPluginName: String = "卡方特征选择"
}
class ChiSqSelectorModel extends Model with HasFeaturesColParam with HasOutputParam{
  private var model:MLModel = _
  def this(model:MLModel) ={
    this()
    this.model = model
  }
  private case class Data(selectedFeatures: Seq[Int])
  private[bds] def toSave(path:String):this.type ={
    val spark = SparkSession.builder().getOrCreate()
    model.save(spark.sparkContext,path)
    this
  }
  /**
    * 模型的读取方法
    * 这个方法用于将model本身加载为保存在path位置的模型
    * 这个方法应该在transform中被自身调用
    *
    * @param path 读取地址
    */
  override protected def loadFrom(path: String): ChiSqSelectorModel.this.type = {
    val spark = SparkSession.builder().getOrCreate()
    this.model = MLModel.load(spark.sparkContext,path)
    this
  }

  /**
    * 变换的执行接口，从待变换的data转化为变换后的data
    *
    * @param data 待变换的表
    * @return 变换后的表
    */
  override def transform(data: DataFrame): DataFrame = {
    if(model==null)loadFrom(getLoadPath)
    // TODO: Make the transformer natively in ml framework to avoid extra conversion.
    val transformer: Vector => Vector = v => model.transform(OldVectors.fromML(v)).asML
    val selector = udf(transformer)
    data.withColumn(getOutputCol, selector(col(getFeaturesCol)))
  }

  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  override def getNecessaryParamList: Array[ParamFromUser] = {
    getDefaultParamList.toArray
  }
  override def getPluginName: String = "卡方特征选择模型"
}
trait ChiSqSelectorParam extends HasOutputParam with HasLabelColParam with HasFeaturesColParam{
  final val NUM_TOP_FEATURES_KEY = "numTopFeatures"
  def numTopFeaturesFromUser:ParamFromUser ={
    val res = ParamFromUser.Integer(NUM_TOP_FEATURES_KEY,"请输入保留特征数#Number of features that selector will select, ordered by ascending p-value. If the" +
      " number of features is < numTopFeatures, then this will select all features.")>=1
    res.fix.addDefaultValue("50")
  }
  def getNumTopFeatures:Int = getValueParam(NUM_TOP_FEATURES_KEY).intValue()
  def setNumTopFeatures(num:Int)={
    assert(num>=1)
    addParam(NUM_TOP_FEATURES_KEY,num)
  }
  setNumTopFeatures(50)


  ///////////////////////////////
  /////////
  //////////////////////////////
  final val PERCENTILE_KEY ="percentile"
//  def percentileFromUser:ParamFromUser={
//    val res = ParamFromUser.Double(PERCENTILE_KEY,"Percentile of features that selector will select, ordered by ascending p-value.") in (0,1)
//    res.fix.addDefaultValue("0.1")
//  }
  def getPercentile:Double = getValueParam(PERCENTILE_KEY).doubleValue()
  def setPercentile(num:Double)={
    assert(num<=1)
    assert(num>=0)
    addParam(PERCENTILE_KEY,num)
  }
  
  ///////////////////////////////
  /////////
  //////////////////////////////
  final val FPR_KEY = "fpr"

//  def fprFromUser:ParamFromUser={
//    val res = ParamFromUser.Double(FPR_KEY,"The highest p-value for features to be kept.") in (0,1)
//    res.fix.addDefaultValue("0.05")
//  }
  def getFpr:Double = getValueParam(FPR_KEY).doubleValue()
  def setFpr(num:Double)={
    assert(num<=1)
    assert(num>=0)
    addParam(FPR_KEY,num)
  }
  setFpr(0.05)

  ///////////////////////////////
  /////////
  //////////////////////////////
  final val FDR_KEY = "fdr"

//  def fdrFromUser:ParamFromUser={
//    val res = ParamFromUser.Double(FDR_KEY,"The upper bound of the expected false discovery rate.") in (0,1)
//    res.fix.addDefaultValue("0.05")
//  }
  def getFdr:Double = getValueParam(FDR_KEY).doubleValue()
  def setFdr(num:Double)={
    assert(num<=1)
    assert(num>=0)
    addParam(FDR_KEY,num)
  }
  setFdr(0.05)

  ///////////////////////////////
  /////////
  //////////////////////////////
  final val FWE_KEY = "fwe"

//  def fweFromUser:ParamFromUser={
//    val res = ParamFromUser.Double(FWE_KEY,"The upper bound of the expected family-wise error rate.") in (0,1)
//    res.fix.addDefaultValue("0.05")
//  }
  def getFwe:Double = getValueParam(FWE_KEY).doubleValue()
  def setFwe(num:Double)={
    assert(num<=1)
    assert(num>=0)
    addParam(FWE_KEY,num)
  }
  setFwe(0.05)

  ///////////////////////////////
  /////////
  //////////////////////////////

  final val SELECTOR_TYPE_KEY = "selectorType"
  val SELECTOR_TYPES = Array("numTopFeatures","percentile","fpr","fdr","fwe")
  def selectorTypeFromUser:ParamFromUser={
    ParamFromUser.SelectBox(SELECTOR_TYPE_KEY,"The selector type of the ChisqSelector. " +
      "Supported options: "+SELECTOR_TYPES.mkString(", "),SELECTOR_TYPES:_*)
  }

  def getSelectorType:String = $(SELECTOR_TYPE_KEY)
  def setSelectorType(tp:String)={
    assert(SELECTOR_TYPES.contains(tp))
    addParam(SELECTOR_TYPE_KEY,tp)
  }

  val PARAM_FOR_SELECTOR_KEY = "param.chisq.sl"
  def paramForSelectorFromUser:ParamFromUser={
    val res = ParamFromUser.Double(PARAM_FOR_SELECTOR_KEY,"请设置参数#根据前面选择的selector type设置参数："+
    "numTopFeatures: 只保留x个最相关特征,参数为>=1的整数,默认50。"+
    "percentile:只保留x%的最相关特征，参数为[0,1]的小数,默认0.1。"+
    "fpr:相关性>x的特征将被保留，参数为[0,1]的小数，默认0.05。"+
    "fdr:The upper bound of the expected false discovery rate. default = 0.05。"+
    "fwe:The upper bound of the expected family-wise error rate. default = 0.05。") in (0,1) orMore "parseInt(a)===t&&t>=1"
    res.fix
  }
  override def addParam(key:String,value:Any):this.type={
    super.addParam(key,value)
    if(key!=null&&((key==PARAM_FOR_SELECTOR_KEY&& $(SELECTOR_TYPE_KEY)!=null)||(key == SELECTOR_TYPE_KEY && $(PARAM_FOR_SELECTOR_KEY)!=null))){
      $(SELECTOR_TYPE_KEY) match{
        case NUM_TOP_FEATURES_KEY=>
          setNumTopFeatures(getValueParam(PARAM_FOR_SELECTOR_KEY).intValue())
        case PERCENTILE_KEY=>
          setPercentile(getValueParam(PARAM_FOR_SELECTOR_KEY).doubleValue())
        case FPR_KEY=>
          setFpr(getValueParam(PARAM_FOR_SELECTOR_KEY).doubleValue())
        case FDR_KEY=>
          setFdr(getValueParam(PARAM_FOR_SELECTOR_KEY).doubleValue())
        case FWE_KEY=>
          setFwe(getValueParam(PARAM_FOR_SELECTOR_KEY).doubleValue())
        case _ =>
          throw new IllegalArgumentException("Unchecked Param SELECTOR_TYPE which is "+$(SELECTOR_TYPE_KEY)+" not in "+SELECTOR_TYPES.mkString("["," ,","]"))
      }
    }
    this
  }

}