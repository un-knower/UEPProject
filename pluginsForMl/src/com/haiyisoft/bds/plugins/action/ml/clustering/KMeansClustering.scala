package com.haiyisoft.bds.plugins.action.ml.clustering

import com.haiyisoft.bds.api.action.ModelTraining
import com.haiyisoft.bds.api.data.Model
import com.haiyisoft.bds.api.data.trans.DataFeaturesInter
import com.haiyisoft.bds.api.param.col.{HasFeaturesColParam, HasOutputParam}
import com.haiyisoft.bds.api.param.ml.{HasMaxIterParam, HasSeedParam}
import com.haiyisoft.bds.api.param.ParamFromUser
import org.apache.spark.ml.clustering.{KMeans => MLTraining, KMeansModel => MLModel}
import org.apache.spark.mllib
import org.apache.spark.sql.DataFrame

/**
  * Created by Namhwik on 2018/3/16.
  */
class KMeansClustering extends ModelTraining[KMeansClusteringModel] with DataFeaturesInter with HasSeedParam with HasMaxIterParam {
  final val initMode = mllib.clustering.KMeans.K_MEANS_PARALLEL

  override def transform(data: DataFrame): DataFrame = {
    val model = fit(data)
    save(model)
    model
      .setFeaturesCol(getInputColsImpl)
      .setOutputCol(getOutputCol)
      .transform(data)
  }

  override def fit(data: DataFrame): KMeansClusteringModel = {
    val kms = new MLTraining()
    val model: MLModel = {
      if (hasSeed) kms.setSeed(getSeed)
      kms
        .setMaxIter(getMaxIter)
        .setInitMode(initMode)
        .setFeaturesCol(getInputColsImpl)
        .setK(getK)
        .fit(data)
    }
    val kMeansClusteringModel = new KMeansClusteringModel()
    kMeansClusteringModel.setModel(model)
    kMeansClusteringModel
  }

  def getK: Int = getValueParam("KMEANNUM").intValue()

  def setK(k: Int): this.type = {
    addParam("KMEANNUM", k)
    this
  }

  override def getNecessaryParamList: Array[ParamFromUser] = {
    val input = featuresColFromUser.addDefaultValue("features").setNeedSchema(true).setShortName("选择特征列")
    val output = outputColFromUser("ml.Vector").addDefaultValue("features").setShortName("输出列")
    val k = ParamFromUser.Integer("KMEANNUM", "请输入K值#最终聚合的类的个数. " +
      "必须 > 1.").>(1).fix
      .addDefaultValue("3")
      .setShortName("聚类个数")
    val savePath = ParamFromUser.save.savePath("请输入模型的保存路径#模型的保存路径,如果无需保存模型则保留“DO NOT SAVE”")
      .setShortName("模型保存路径")
    val seed = seedFromUser.setShortName("随机数种子")
    val maxIter = maxIterFromUser.setShortName("最大迭代次数")
    Array(input, output, k, maxIter, seed, savePath)
  }

  override def save(stored: KMeansClusteringModel): Unit = {
    if (getSavePath != "DO NOT SAVE")
      stored.saveTo(getSavePath)
  }

  override def getPluginName: String = "K均值"
}

class KMeansClusteringModel extends Model with HasFeaturesColParam with HasOutputParam {
  var model: MLModel = _

  private[action] def setModel(model: MLModel): Unit = this.model = model

  private[action] def saveTo(path: String): Unit = model.write.overwrite().save(path)

  override def loadFrom(path: String): this.type = {
    this.model = MLModel.load(path)
    this
  }

  override def transform(data: DataFrame): DataFrame = {
    if (model == null) loadFrom(getLoadPath)
    this.model
      .setFeaturesCol(getFeaturesCol)
      .setPredictionCol(getOutputCol)
      .transform(data)
  }

  //override def setSavePath(savePath: String): Unit = paramMap.put(savePathKey, savePath)

  override def getNecessaryParamList: Array[ParamFromUser] = {
    getDefaultParamList.toArray
  }

  override def getPluginName: String = "K均值模型"
}