package com.haiyisoft.bds.plugins.action.ml.clasification

import com.haiyisoft.bds.api.action.ModelTraining
import com.haiyisoft.bds.api.data.Model
import com.haiyisoft.bds.api.param._
import com.haiyisoft.bds.api.param.col.{HasFeaturesColParam, HasLabelColParam, HasOutputParam}
import com.haiyisoft.bds.api.param.ml._
import org.apache.spark.ml.classification.{MultilayerPerceptronClassifier => MLTraining, MultilayerPerceptronClassificationModel => MLModel}
import org.apache.spark.sql.DataFrame

/**
  * Created by XingxueDU on 2018/3/22.
  */
class MultilayerPerceptronClassifierAction extends ModelTraining[MultilayerPerceptronClassifierModel]with HasFeaturesColParam with HasLabelColParam
  with HasSeedParam with HasTolParam with HasMaxIterParam{

  protected val mlpc = new MLTraining()

  /**
    * 模型训练
    *
    * @param data 训练集
    * @return 生成的模型
    */
  override def fit(data: DataFrame): MultilayerPerceptronClassifierModel = {
    if(hasSeed)mlpc.setSeed(getSeed)
    val model = mlpc.setMaxIter(getMaxIter)
      .setTol(getTol)
      .setFeaturesCol(getFeaturesCol)
      .setLabelCol(getLabelCol)
      .fit(data)
    new MultilayerPerceptronClassifierModel(model)
  }

  /**
    * 保存对象.注意：保存的是结果而不是自身
    *
    * @param stored 被保存对象（dataFrame或者Model）
    */
  override def save(stored: MultilayerPerceptronClassifierModel): Unit = {
    stored.saveTo(getSavePath)
  }

  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  override def getNecessaryParamList: Array[ParamFromUser] = {
    val label = labelColFromUser
    val features = featuresColFromUser
    val maxIter = maxIterFromUser
    val tol = tolFromUser
    val seed = seedFromUser
    Array(label,features,maxIter,tol,seed)
  }

  override def getPluginName: String = "MLPC"
}
class MultilayerPerceptronClassifierModel extends Model with HasFeaturesColParam with HasOutputParam{
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
  override protected def loadFrom(path: String): MultilayerPerceptronClassifierModel.this.type = {
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
    Array(features,output)
  }

  override def getPluginName: String = "MLPC模型"
}