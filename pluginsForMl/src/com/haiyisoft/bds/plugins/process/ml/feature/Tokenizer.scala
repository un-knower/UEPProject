package com.haiyisoft.bds.plugins.process.ml.feature

import com.haiyisoft.bds.api.data.trans.DataProcessInter
import com.haiyisoft.bds.api.param.ParamFromUser
import com.haiyisoft.bds.api.param.col.{HasOneInputParamByName, HasOutputParam}
import org.apache.spark.ml.feature.{Tokenizer => MLTransformer}
import org.apache.spark.sql.DataFrame

/**
  * Created by XingxueDU on 2018/3/21.
  */
class Tokenizer extends DataProcessInter with HasOneInputParamByName with HasOutputParam {

  override def transform(data: DataFrame): DataFrame = {
    val inputCol = getInputColsImpl
    val outputCol = getOutputCol
    val indexer = new MLTransformer()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
    indexer.transform(data)
  }

  override def getNecessaryParamList: Array[ParamFromUser] = {
    val inputCol = oneInputColFromUser("String")
    val outputCol = outputColFromUser("Double")
    Array(inputCol,outputCol)
  }

  override def getPluginName: String = "英文分词器"
}
