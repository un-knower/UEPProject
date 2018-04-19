package com.haiyisoft.bds.plugins.action.ml.clasification

import com.haiyisoft.bds.api.action.ModelTraining
import com.haiyisoft.bds.api.data.Model
import com.haiyisoft.bds.api.param._
import com.haiyisoft.bds.api.param.col.{HasFeaturesColParam, HasLabelColParam, HasOutputParam, HasProbabilityColParam}
import org.apache.spark.ml.classification.{NaiveBayes => MLTraining, NaiveBayesModel => MLModel}
import org.apache.spark.sql.DataFrame

/**
  * Created by XingxueDU on 2018/3/22.
  */
class NaiveBayesAction extends ModelTraining[NaiveBayesModel]with HasFeaturesColParam with HasLabelColParam{

  protected val nbs = new MLTraining()

  /**
    * 模型训练
    *
    * @param data 训练集
    * @return 生成的模型
    */
  override def fit(data: DataFrame): NaiveBayesModel = {
    val model = nbs
      .setFeaturesCol(getFeaturesCol)
      .setLabelCol(getLabelCol)
        .setModelType(getModelType)
        .setSmoothing(getSmoothing)
      .fit(data)
    new NaiveBayesModel(model)
  }

  /**
    * 保存对象.注意：保存的是结果而不是自身
    *
    * @param stored 被保存对象（dataFrame或者Model）
    */
  override def save(stored: NaiveBayesModel): Unit = {
    stored.saveTo(getSavePath)
  }

  def getModelType:String = getStringParam("modelType")
  def getSmoothing:Double = getValueParam("smoothing").doubleValue()
  def setModelType(modelType:String):this.type = {
    assert("multinomial"==modelType||"bernoulli"==modelType)
    addParam("modelType",modelType)
  }
  def setSmoothing(value:Double):this.type={
    assert(value>=0d)
    addParam("smoothing",value)
  }
  setSmoothing(1d)
  setModelType("multinomial")
  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  override def getNecessaryParamList: Array[ParamFromUser] = {
    val label = labelColFromUser
    val features = featuresColFromUser
    val modelType = ParamFromUser.SelectBox("modelType","The model type " +
      "which is a string (case-sensitive). Supported options: multinomial (default) and bernoulli.","multinomial","bernoulli")
    val smoothing = ParamFromUser.Double("smoothing", "The smoothing parameter.").>=(0).fix.addDefaultValue("1")
    Array(label,features,modelType,smoothing)
  }

  override def getPluginName: String = "贝叶斯"
}

class NaiveBayesModel extends Model with HasFeaturesColParam with HasOutputParam with HasProbabilityColParam{
  protected var model:MLModel = _

  def this(model:MLModel)={
    this()
    this.model = model
  }
  private[bds] def saveTo(path:String) = {
    model.write.overwrite().save(path)
  }

  /**
    * 模型的读取方法
    * 这个方法用于将model本身加载为保存在path位置的模型
    * 这个方法应该在transform中被自身调用
    *
    * @param path 读取地址
    */
  override protected def loadFrom(path: String): NaiveBayesModel.this.type = {
    this.model = MLModel.load(path)
    this
  }

  /**
    * 变换的执行接口，从待变换的data转化为变换后的data
    *
    * @param data 待变换的表
    * @return 变换后的表
    */
  override def transform(data: DataFrame): DataFrame = {
    if(model == null)loadFrom(getLoadPath)
    if(hasProbabilityCol)model.setProbabilityCol(getProbabilityCol)
    model.setFeaturesCol(getFeaturesCol)
      .setPredictionCol(getOutputCol)
      .transform(data)
  }

  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  override def getNecessaryParamList: Array[ParamFromUser] = {
    val features = featuresColFromUser
    val output = outputColFromUser("Double")
    val proba = probabilityColFromUser
    Array(features,output,proba)
  }

  override def getPluginName: String = "贝叶斯模型"
}