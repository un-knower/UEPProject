package com.haiyisoft.bds.plugins.process.ml.feature

import com.haiyisoft.bds.api.data.trans.Data2VecInter
import com.haiyisoft.bds.api.param.ParamFromUser
import com.haiyisoft.bds.api.param.col.HasOneInputParamByName
import org.apache.spark.ml.feature.{OneHotEncoder => MLTransformer}
import org.apache.spark.sql.DataFrame

import scala.util.Try

/**
  * Created by Namhwik on 2018/3/19.
  */
class OntHotEncoder extends Data2VecInter with HasOneInputParamByName{

  override def transform(data: DataFrame): DataFrame = {
    val inputCol = getInputColsImpl
    val outputCol = getOutputCol
    val encoder = new MLTransformer()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
    val dt = Try{isDropLast}
    if(dt.isSuccess)encoder.setDropLast(dt.get)
    encoder.transform(data)
  }

  override def getNecessaryParamList: Array[ParamFromUser] = {
    val input = oneInputColFromUser("Double")
    val output = outputColFromUser("ml.Vector")
    val dropLast = ParamFromUser.Boolean("OntHotEncoder.dropLast","Whether drop Last#Whether to drop the last category in the encoded vector (default: true)").addDefaultValue("true")
    Array(input,output,dropLast)
  }
  def isDropLast:Boolean ={
    getBoolParam("OntHotEncoder.dropLast")
  }

  override def getPluginName: String = "OntHot编码"
}
