package com.haiyisoft.bds.plugins.process.ml.feature

import com.haiyisoft.bds.api.data.trans.DataFeaturesInter
import com.haiyisoft.bds.api.param.ParamFromUser
import org.apache.spark.ml.feature.{ElementwiseProduct => MLTransformer}
import org.apache.spark.sql.DataFrame

/**
  * Created by XingxueDU on 2018/3/21.
  */
class ElementwiseProduct extends DataFeaturesInter{
  /**
    * 变换的执行接口，从待变换的data转化为变换后的data
    *
    * @param data 待变换的表
    * @return 变换后的表
    */
  override def transform(data: DataFrame): DataFrame = {
    val weight = getScalingVec
    val ts = new MLTransformer()
      .setInputCol(getInputColsImpl)
      .setOutputCol(getOutputCol)
    if(weight!=null) ts.setScalingVec(weight)
    ts.transform(data)
  }

  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  override def getNecessaryParamList: Array[ParamFromUser] = {
    val input = featuresColFromUser
    val output = outputColFromUser("ml.Vector").addDefaultValue("features")
    val scalingVec = ParamFromUser.Double("weight","请输入权重值#请分别输入每列的权重值（Double）").>=(0).<=(1).fix.setAllowMultipleResult(true)
    Array(input,output,scalingVec)
  }

  def getScalingVec:org.apache.spark.ml.linalg.Vector={
    val res = getArrayDoubleParam("weight")
    if(res == null)null
    else{
      org.apache.spark.ml.linalg.Vectors.dense(res)
    }
  }

  override def getPluginName: String = "元素智能乘积"
}
