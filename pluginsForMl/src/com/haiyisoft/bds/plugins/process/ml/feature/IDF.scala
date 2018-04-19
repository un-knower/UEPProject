package com.haiyisoft.bds.plugins.process.ml.feature

import com.haiyisoft.bds.api.data.trans.DataFeaturesInter
import com.haiyisoft.bds.api.param.ParamFromUser
import com.haiyisoft.bds.api.param.col.{HasOutputParam, HasFeaturesColParam}
import org.apache.spark.ml.feature.{IDF => MLTransformer}
import org.apache.spark.sql.DataFrame

/**
  * Created by XingxueDU on 2018/3/23.
  */
class IDF extends DataFeaturesInter with HasFeaturesColParam with HasOutputParam{
  /**
    * 变换的执行接口，从待变换的data转化为变换后的data
    *
    * @param data 待变换的表
    * @return 变换后的表
    */
  override def transform(data: DataFrame): DataFrame = {
    val idf = new MLTransformer()
        .setMinDocFreq(getMinDocFreq)
      .setInputCol(getFeaturesCol)
      .setOutputCol(getOutputCol)
    val idfModel = idf.fit(data)

    val rescaledData = idfModel.transform(data)
    rescaledData
  }

  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  override def getNecessaryParamList: Array[ParamFromUser] = {
    val min = ParamFromUser.Integer("minDocFreq","请输入最少文档数量#minimum number of documents in which a term should appear for filtering").fix.addDefaultValue("0")
    getDefaultParamList.toArray:+min
  }

  def setMinDocFreq(freq:Int):this.type={
    addParam("minDocFreq",freq)
  }
  def getMinDocFreq:Int = getValueParam("minDocFreq").intValue()
  setMinDocFreq(0)

  override def getPluginName: String = "IDF逆文档频率"
}
