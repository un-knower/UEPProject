package com.haiyisoft.bds.plugins.process.ml.feature

import com.haiyisoft.bds.api.data.trans.Data2VecInter
import com.haiyisoft.bds.api.param.ParamFromUser
import com.haiyisoft.bds.api.param.col.HasOneInputParamByName
import com.haiyisoft.bds.api.param.ml.HasSeedParam
import org.apache.spark.ml.feature.{Word2Vec => MLTransformer}
import org.apache.spark.sql.DataFrame

/**
  * Created by XingxueDU on 2018/3/21.
  */
class Word2Vec extends Data2VecInter with HasOneInputParamByName with HasSeedParam{

  override def transform(data: DataFrame): DataFrame = {
    val inputCol = getInputColsImpl
    val outputCol = getOutputCol
    val indexer = new MLTransformer()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setMaxSentenceLength(getMaxSentenceLength)
      .setMinCount(getMinCount)
      .setNumPartitions(getNumPartitions)
      //      .setStepSize(getStepSize)
      .setVectorSize(getVectorSize)
      .setWindowSize(getWindowSize)
    if(hasSeed)indexer.setSeed(getSeed)
    indexer.fit(data).transform(data)
  }

  override def getNecessaryParamList: Array[ParamFromUser] = {
    val inputCol = oneInputColFromUser("String")
    val outputCol = outputColFromUser("Double")

    val MaxSentenceLength = ParamFromUser.Integer("MaxSentenceLength","请输入最大句子长度#Maximum length " +
      "(in words) of each sentence in the input data. Any sentence longer than this threshold will " +
      "be divided into chunks up to the size.").>=(0).fix
      .addDefaultValue("1000")
    val MinCount = ParamFromUser.Integer("MinCount","请输入最小出现次数#the minimum number of times a token must " +
      "appear to be included in the word2vec model's vocabulary").>=(0).fix
      .addDefaultValue("5")
    val NumPartitions = ParamFromUser.Integer("NumPartitions","请输入文档数#number of partitions for sentences of words").>=(0).fix
      .addDefaultValue("1")
    val seed = seedFromUser
    //    val StepSize = ParamFromUser.Double("StepSize","Step size to be used for each iteration of optimization",true,true)
    val VectorSize =  ParamFromUser.Integer("VectorSize","请输入矩阵维度#the dimension of codes after transforming from words").>=(0).fix
      .addDefaultValue("100")
    val WindowSize =  ParamFromUser.Integer("WindowSize","请输入窗口大小#the window size (context words from [-window, window])").>=(0).fix
      .addDefaultValue("5")

    Array(inputCol,outputCol,MaxSentenceLength,MinCount,NumPartitions,VectorSize,WindowSize,seed)
  }
  def getMaxSentenceLength:Int = {
    getValueParam("MaxSentenceLength").intValue()
  }
  def getMinCount :Int = {
    getValueParam("MinCount").intValue()
  }
  def getNumPartitions :Int = {
    getValueParam("NumPartitions").intValue()
  }
  //  def getStepSize :Double = {
  //    getValueParam("StepSize").doubleValue()
  //  }
  def getVectorSize :Int = {
    getValueParam("VectorSize").intValue()
  }
  def getWindowSize :Int = {
    getValueParam("WindowSize").intValue()
  }

  override def getPluginName: String = "文字转向量"
}
