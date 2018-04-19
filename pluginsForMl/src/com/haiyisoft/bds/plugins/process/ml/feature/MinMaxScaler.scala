package com.haiyisoft.bds.plugins.process.ml.feature

import com.haiyisoft.bds.api.data.trans.DataFeaturesInter
import com.haiyisoft.bds.api.param.ParamFromUser
import org.apache.spark.ml.feature.{MinMaxScaler => MLTransformer}
import org.apache.spark.sql.DataFrame

/**
  * Created by XingxueDU on 2018/3/21.
  */
class MinMaxScaler extends DataFeaturesInter {
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
      .setMin(getMin)
      .setMax(getMax)
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
    val min = ParamFromUser
      .Double("min", "请输入最小值#标准化后的数据最小值").fix.addDefaultValue("0")
      .setShortName("最小值")
    val max = ParamFromUser
      .Double("max", "请输入最大值#标准化后的数据最大值")
      .fix.addDefaultValue("1")
      .setShortName("最大值")
    Array(input, output, min, max)
  }

  def getMin: Double = getValueParam("min").doubleValue()

  def getMax: Double = getValueParam("max").doubleValue()

  override def getPluginName: String = "最值标准化"
}
