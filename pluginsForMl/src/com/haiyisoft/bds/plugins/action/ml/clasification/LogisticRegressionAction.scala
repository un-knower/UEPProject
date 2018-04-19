package com.haiyisoft.bds.plugins.action.ml.clasification

import com.haiyisoft.bds.api.action.ModelTraining
import com.haiyisoft.bds.api.data.Model
import com.haiyisoft.bds.api.param._
import com.haiyisoft.bds.api.param.col.{HasFeaturesColParam, HasLabelColParam, HasOutputParam, HasProbabilityColParam}
import com.haiyisoft.bds.api.param.ml._
import org.apache.spark.ml.classification.{LogisticRegression => MLTraining, LogisticRegressionModel => MLModel}
import org.apache.spark.sql.DataFrame

/**
  * Created by XingxueDU on 2018/3/21.
  */
class LogisticRegressionAction extends ModelTraining[LogisticRegressionModel]
  with HasLabelColParam with HasFeaturesColParam
  with HasMaxIterParam with HasRegParam with HasTolParam with HasNotNecessaryParam
  with HasThresholdParam {
  val lr = new MLTraining()

  /**
    * 模型训练
    *
    * @param data 训练集
    * @return 生成的模型
    */
  override def fit(data: DataFrame): LogisticRegressionModel = {
    lr.setMaxIter(getMaxIter)
      .setFeaturesCol(getFeaturesCol)
      .setLabelCol(getLabelCol)
      .setRegParam(getRegParam)
      .setThreshold(getThreshold)
      .setTol(getTol)
    val model = lr.fit(data)
    new LogisticRegressionModel().setModel(model)
  }

  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  override def getNecessaryParamList: Array[ParamFromUser] = {
    val features = featuresColFromUser
    val label = labelColFromUser
    val maxIter = maxIterFromUser
    val tol = tolFromUser
    val reg = regParamFromUser
    val threshold = thresholdFromUser
    //    val more = moreFromUser
    Array(label, features, maxIter, tol, reg, threshold)
  }


  /**
    * 保存对象.注意：保存的是结果而不是自身
    *
    * @param stored 被保存对象（dataFrame或者Model）
    */
  override def save(stored: LogisticRegressionModel): Unit = {
    stored.saveTo(getSavePath)
  }

  override def getPluginName: String = "逻辑回归"
}

class LogisticRegressionModel extends Model with HasFeaturesColParam with HasOutputParam
  with HasProbabilityColParam {
  var model: MLModel = null

  private[action] def setModel(mLModel: MLModel): this.type = {
    this.model = mLModel
    this
  }

  private[action] def saveTo(path: String): this.type = {
    this.model.write.overwrite().save(path)
    this
  }

  /**
    * 模型的读取方法
    *
    * @param path 读取地址
    * @return 模型，通常是本身，也可以重新构建一个
    */
  override def loadFrom(path: String): this.type = {
    model = MLModel.load(path)
    this
  }


  /**
    * 预测方法。通过模型对数据进行预测
    *
    * @param data 经过流程传入的数据
    * @return 训练结果
    */
  override def transform(data: DataFrame): DataFrame = {
    if (model == null) loadFrom(getLoadPath)
    if (hasProbabilityCol) model.setProbabilityCol(getProbabilityCol)
    model.setPredictionCol(getOutputCol)
      .setFeaturesCol(getFeaturesCol)
    model.transform(data)
  }

  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  override def getNecessaryParamList: Array[ParamFromUser] = {
    getDefaultParamList.toArray :+ probabilityColFromUser
  }

  override def getPluginName: String = "逻辑回归模型"
}