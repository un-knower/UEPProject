package com.haiyisoft.bds.plugins.process.ml.feature

import com.haiyisoft.bds.api.data.trans.DataProcessInter
import com.haiyisoft.bds.api.param.ParamFromUser
import com.haiyisoft.bds.api.param.col.{HasOneInputParamByName, HasOutputParam}
import org.apache.spark.ml.feature.{Binarizer => MLTransformer}
import org.apache.spark.sql.DataFrame

/**
  * Created by XingxueDU on 2018/3/21.
  */
class Binarizer extends DataProcessInter with HasOneInputParamByName with HasOutputParam {

  override def transform(data: DataFrame): DataFrame = {
    val inputCol = getInputColsImpl
    val outputCol = getOutputCol
    val indexer = new MLTransformer()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setThreshold(getThreshold)
    indexer.transform(data)
  }

  override def getNecessaryParamList: Array[ParamFromUser] = {
    val inputCol = oneInputColFromUser("String")
    val outputCol = outputColFromUser("Double")

    val Threshold = ParamFromUser.Double("Threshold","请输入阈值#threshold used to binarize continuous features").<=(0).>=(0).fix.addDefaultValue("0.0")

    Array(inputCol,outputCol,Threshold)
  }

  def getThreshold:Double ={
    val res = getValueParam("Threshold").doubleValue()
    if(res > 1)1d else res
  }

  override def getPluginName: String = "二进制化分箱"
}
