package com.haiyisoft.bds.plugins.process.ml.feature

import com.haiyisoft.bds.api.data.trans.DataProcessInter
import com.haiyisoft.bds.api.param.ParamFromUser
import com.haiyisoft.bds.api.param.col.{HasOneInputParamByName, HasOutputParam}
import org.apache.spark.ml.feature.{StopWordsRemover => MLTraining}
import org.apache.spark.sql.DataFrame


/**
  * Created by XingxueDU on 2018/3/23.
  */
class StopWordsRemover extends DataProcessInter with HasOneInputParamByName with HasOutputParam{
  private val training = new MLTraining()



  /**
    * 变换的执行接口，从待变换的data转化为变换后的data
    *
    * @param data 待变换的表
    * @return 变换后的表
    */
  override def transform(data: DataFrame): DataFrame = {
    training.setCaseSensitive(getCaseSensitive)
      .setStopWords(getStopWords)
      .setInputCol(getInputColsImpl)
      .setOutputCol(getOutputCol)
      .transform(data)
  }

  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  override def getNecessaryParamList: Array[ParamFromUser] = {
    val input = oneInputColFromUser("Array[String]")
    val output = outputColFromUser("Array[String]")
    val stopWord = stopWordsFromUser
    val caseSensitive = caseSensitiveFromUser
    Array(input,output,stopWord,caseSensitive)
  }

  /////////////////
  /////stopword
  /////////////////
  val STOP_WORD_KEY = "stopWords"
  def stopWordsFromUser = ParamFromUser.String(STOP_WORD_KEY,"请输入停用词#the words to be filtered out").setAllowMultipleResult(true)
  def setStopWords(words:Array[String]):this.type ={
    addParam(STOP_WORD_KEY,words)
    this
  }
  def getStopWords:Array[String]= getArrayStringParam(STOP_WORD_KEY)

  /////////////////////
  /////caseSensitive
  ////////////////////
  val CASE_SENSITIVE_KEY="caseSensitive"
  def caseSensitiveFromUser = ParamFromUser.Boolean(CASE_SENSITIVE_KEY,"请输入是否区分大小写#whether to do a case-sensitive comparison over the stop words").addDefaultValue("false")
  def getCaseSensitive:Boolean = getBoolParam(CASE_SENSITIVE_KEY)
  def setCaseSensitive(c:Boolean):this.type = {
    addParam(CASE_SENSITIVE_KEY,c)
    this
  }

  override def getPluginName: String = "停用词处理处理"
}
