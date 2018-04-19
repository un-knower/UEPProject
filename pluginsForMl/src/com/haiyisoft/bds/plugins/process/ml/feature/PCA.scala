package com.haiyisoft.bds.plugins.process.ml.feature

import com.haiyisoft.bds.api.data.trans.DataProcessInter
import com.haiyisoft.bds.api.param.ParamFromUser
import com.haiyisoft.bds.api.param.col.{HasOneInputParamByName, HasOutputParam}
import org.apache.spark.ml.feature.{PCA => MLTransformer}
import org.apache.spark.sql.DataFrame

/**
  * Created by XingxueDU on 2018/3/21.
  */
class PCA extends DataProcessInter with HasOneInputParamByName with HasOutputParam {

  override def transform(data: DataFrame): DataFrame = {
    val inputCol = getInputColsImpl
    val outputCol = getOutputCol
    val indexer = new MLTransformer()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setK(getK)
    indexer.fit(data).transform(data)
  }

  override def getNecessaryParamList: Array[ParamFromUser] = {
    val inputCol = oneInputColFromUser("String")
    val outputCol = outputColFromUser("Double")
    val K = ParamFromUser.Integer("PCA.K","请输入K值#PCA可以找出特征中最主要的特征，把原来的n个特征用k(k < n)个特征代").>=(1).fix
    Array(inputCol,outputCol,K)
  }

  def getK:Int={
    getValueParam("PCA.K").intValue()
  }

  override def getPluginName: String = "文本主成分分析"
}
