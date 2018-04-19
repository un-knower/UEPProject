package com.haiyisoft.bds.plugins.process.ml.feature

import com.haiyisoft.bds.api.data.trans.DataFeaturesInter
import com.haiyisoft.bds.api.param.ParamFromUser
import org.apache.spark.ml.feature.{MaxAbsScaler => MLTransformer}
import org.apache.spark.sql.DataFrame

/**
  * Created by XingxueDU on 2018/3/21.
  */
class MaxAbsScaler extends DataFeaturesInter{
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
    Array(input,output)
  }

  override def getPluginName: String = "绝对值标准化"
}
