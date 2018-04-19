package com.haiyisoft.bds.plugins.process.ml.feature

import com.haiyisoft.bds.api.data.trans.DataProcessInter
import com.haiyisoft.bds.api.param.ParamFromUser
import com.haiyisoft.bds.api.param.col.{HasOneInputParamByName, HasOutputParam}
import org.apache.spark.ml.feature.{StringIndexer => MLTransformer}
import org.apache.spark.sql.DataFrame

/**
  * Created by Namhwik on 2018/3/19.
  *
  * 将以String记载的分类数据列转化为Double的label列
  *
  * @see [[org.apache.spark.ml.feature.StringIndexer]]
  *
  * 可以通过[[IndexToString]]还原
  * @author Zhai Te
  * @author DU Xingxue
  * @author XU Jing
  */
class StringIndex extends DataProcessInter with HasOneInputParamByName with HasOutputParam {

  override def transform(data: DataFrame): DataFrame = {
    val inputCol = getInputColsImpl
    val outputCol = getOutputCol
    val hi = getHandleInvalid
    val indexer = new MLTransformer()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
    if(hi!=null)indexer.setHandleInvalid(hi)
    indexer.fit(data).transform(data)
  }

  override def getNecessaryParamList: Array[ParamFromUser] = {
    val inputCol = oneInputColFromUser("String")
    val outputCol = outputColFromUser("Double")
    val handle = ParamFromUser.SelectBox("handleInvalid","请输入异常处理方法#如何处理异常数据，skip(跳过)，error(作为错误跳出)","skip","error")

    Array(inputCol,outputCol,handle)
  }

  def getHandleInvalid:String={
    if(Set("Options","error").contains($("handleInvalid")))$("handleInvalid")
    else null
  }

  override def getPluginName: String = "字符标签数字化"
}
