package com.haiyisoft.bds.plugins.process.complex

import com.haiyisoft.bds.api.data.trans.Data2VecInter
import com.haiyisoft.bds.api.param.ParamFromUser
import com.haiyisoft.bds.api.param.col.{HasOutputParam, HasLabelColParam, HasOneInputParamByName}
import org.apache.spark.ml.feature.{IDF, HashingTF, Tokenizer}
import org.apache.spark.sql.DataFrame

/**
  * TF-IDF的整合
  * Created by XingxueDU on 2018/3/23.
  */
class TF_IDF extends Data2VecInter with HasOneInputParamByName with HasLabelColParam with HasOutputParam{
  /**
    * 变换的执行接口，从待变换的data转化为变换后的data
    *
    * @param data 待变换的表
    * @return 变换后的表
    */
  override def transform(data: DataFrame): DataFrame = {
    val tokenizer = new Tokenizer().setInputCol(getInputColsImpl).setOutputCol(getOutputCol+"_word")
    val wordsData = tokenizer.transform(data)

    val hashingTF = new HashingTF()
      .setInputCol(getOutputCol+"_word").setOutputCol(getOutputCol+"_rawFeatures").setNumFeatures(getNumFeatures)

    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().setInputCol(getOutputCol+"_rawFeatures").setOutputCol(getOutputCol)
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData
  }

  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  override def getNecessaryParamList: Array[ParamFromUser] = {
    val sentence = ParamFromUser.String(INPUT_COL_KEY,"请输入文章所在列#文章所在列",true)
    val out = outputColFromUser("ml.Vector").addDefaultValue("features")
    val label = labelColFromUser
    val numFeatures = ParamFromUser.Integer("HashingTF.numFeatures","请输入Hash的分桶数量#Hash的分桶数量默认2^20").>(0).fix.addDefaultValue("1048576")

    Array(sentence,out,label,numFeatures)
  }
  def getNumFeatures:Int ={
    val res = getValueParam("HashingTF.numFeatures")
    if(res==null)-1 else res.intValue()
  }
  def setNumFeatures(num:Int):this.type={
    assert(num>0)
    addParam("HashingTF.numFeatures",num)
  }

  override def getPluginName: String = "TF-IDF"
}
