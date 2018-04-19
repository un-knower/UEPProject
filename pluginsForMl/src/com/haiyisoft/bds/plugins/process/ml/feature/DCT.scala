package com.haiyisoft.bds.plugins.process.ml.feature

import com.haiyisoft.bds.api.data.trans.DataProcessInter
import com.haiyisoft.bds.api.param.ParamFromUser
import com.haiyisoft.bds.api.param.col.{HasOneInputParamByName, HasOutputParam}
import org.apache.spark.ml.feature.{DCT => MLTransformer}
import org.apache.spark.sql.DataFrame

/**
  * Created by XingxueDU on 2018/3/21.
  */
class DCT extends DataProcessInter with HasOneInputParamByName with HasOutputParam {

  override def transform(data: DataFrame): DataFrame = {
    val inputCol = getInputColsImpl
    val outputCol = getOutputCol
    val indexer = new MLTransformer()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setInverse(getInverse)
    indexer.transform(data)
  }

  override def getNecessaryParamList: Array[ParamFromUser] = {
    val inputCol = oneInputColFromUser("String")
    val outputCol = outputColFromUser("Double")

    val inverse = ParamFromUser.Boolean("inverse","Set transformer to perform inverse DCT").addDefaultValue("false")

    Array(inputCol,outputCol,inverse)
  }

  def getInverse:Boolean = getBoolParam("inverse")

  override def getPluginName: String = "离散余弦变换"
}
