package com.haiyisoft.bds.plugins.process.ml.feature

import com.haiyisoft.bds.api.data.trans.DataProcessInter
import com.haiyisoft.bds.api.param.ParamFromUser
import com.haiyisoft.bds.api.param.col.{HasOneInputParamByName, HasOutputParam}
import org.apache.spark.ml.feature.{CountVectorizer => MLTransformer}
import org.apache.spark.sql.DataFrame

/**
  * Created by XingxueDU on 2018/3/21.
  */
class CountVectorizer extends DataProcessInter with HasOneInputParamByName with HasOutputParam {

  override def transform(data: DataFrame): DataFrame = {
    val inputCol = getInputColsImpl
    val outputCol = getOutputCol
    val indexer = new MLTransformer()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setMinDF(getMinDF)
      .setMinTF(getMinTF)
      .setBinary(getBinary)
      .setVocabSize(getVocabSize)
    indexer.fit(data).transform(data)
  }

  override def getNecessaryParamList: Array[ParamFromUser] = {
    val inputCol = oneInputColFromUser("String")
    val outputCol = outputColFromUser("Double")

    val MinDF    = ParamFromUser.Double("MinDF","词汇最少存在的文档数#Specifies the minimum number of" +
      " different documents a term must appear in to be included in the vocabulary." +
      " If this is an integer >= 1, this specifies the number of documents the term must" +
      " appear in; if this is a double in [0,1), then this specifies the fraction of documents.").>=(0).fix.addDefaultValue("1.0")
    val MinTF    = ParamFromUser.Double("MinTF","请输入罕见词语阈值#Filter to ignore rare words in" +
      " a document. For each document, terms with frequency/count less than the given threshold are" +
      " ignored. If this is an integer >= 1, then this specifies a count (of times the term must" +
      " appear in the document); if this is a double in [0,1), then this specifies a fraction (out" +
      " of the document's token count). Note that the parameter is only used in transform of" +
      " CountVectorizerModel and does not affect fitting.").>=(0).fix.addDefaultValue("1.0")
    val Binary    = ParamFromUser.Boolean("Binary","请输入是否输出二进制向量#If True, all non zero counts are set to 1.").addDefaultValue("false")
    val VocabSize = ParamFromUser.Integer("VocabSize","请输入最大词汇量#max size of the vocabulary").>(0).addDefaultValue("262144")

    Array(inputCol,outputCol,MinDF,MinTF,Binary,VocabSize)
  }

  def getMinDF:Double ={
    val res = getValueParam("MinDF")
    res.doubleValue()
  }
  def getMinTF:Double ={
    val res = getValueParam("MinTF")
    res.doubleValue()
  }
  def getBinary:Boolean ={
    val res = getBoolParam("Binary")
    res
  }
  def getVocabSize:Int ={
    val res = getValueParam("VocabSize")
    res.intValue()
  }

  override def getPluginName: String = "文本标记计数"
}
