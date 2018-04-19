package com.haiyisoft.bds.plugins.process.ml.feature

import com.haiyisoft.bds.api.data.trans.DataFeaturesInter
import com.haiyisoft.bds.api.param.ParamFromUser
import org.apache.spark.ml.feature.{StandardScaler => MLTransformer}
import org.apache.spark.sql.DataFrame

/**
  * Created by XingxueDU on 2018/3/21.
  */
class StandardScaler extends DataFeaturesInter{
  /**
    * 变换的执行接口，从待变换的data转化为变换后的data
    *
    * @param data 待变换的表
    * @return 变换后的表
    */
  override def transform(data: DataFrame): DataFrame = {
    new MLTransformer()
      .setInputCol(getInputColsImpl)
      .setOutputCol(getOutputCol)
      .setWithMean(getWithMean)
      .setWithStd(getWithStd)
      .fit(data)
      .transform(data)
  }

  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  override def getNecessaryParamList: Array[ParamFromUser] = {
    val input = featuresColFromUser
    val output = outputColFromUser("ml.Vector").addDefaultValue("features")
    val mean = ParamFromUser.Boolean("standardScaler.mean","请输入是否均一化均值#是否将均值平移到0").addDefaultValue("false")
    val std = ParamFromUser.Boolean("standardScaler.std","请输入是否均一化方差#是否将标准差缩放到1").addDefaultValue("true")
    Array(input,output,mean,std)
  }

  def getWithMean:Boolean = getBoolParam("standardScaler.mean")
  def getWithStd:Boolean = getBoolParam("standardScaler.std")

  override def getPluginName: String = "均值方差均一化"
}
