package com.haiyisoft.bds.plugins.process.ml.feature

import com.haiyisoft.bds.api.data.trans.Data2VecInter
import com.haiyisoft.bds.api.param.ParamFromUser
import com.haiyisoft.bds.api.param.col.HasOneInputParamByName
import org.apache.spark.ml.feature.{HashingTF => MLTransformer}
import org.apache.spark.sql.DataFrame

/**
  * Created by XingxueDU on 2018/3/21.
  */
class HashingTF extends Data2VecInter with HasOneInputParamByName {

  override def transform(data: DataFrame): DataFrame = {
    val inputCol = getInputColsImpl
    val outputCol = getOutputCol
    val indexer = new MLTransformer()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setBinary(getBinary)
    if(getNumFeatures>0)indexer.setNumFeatures(getNumFeatures)
    indexer.transform(data)
  }

  override def getNecessaryParamList: Array[ParamFromUser] = {
    val inputCol = oneInputColFromUser("String")
    val outputCol = outputColFromUser("Double")
    val numFeatures = ParamFromUser.Integer("HashingTF.numFeatures","请输入分箱数#请输入Hash的分桶数量，默认2^20").>(0).fix.addDefaultValue("1048576")
    val binary = ParamFromUser.Boolean("HashingTF.binary","请输入是否二进制化#如果为真，则所有非0值将被设置为1").addDefaultValue("false")
    Array(inputCol,outputCol,numFeatures,binary)
  }
  def getNumFeatures:Int ={
    val res = getValueParam("HashingTF.numFeatures")
    if(res==null)-1 else res.intValue()
  }

  def getBinary:Boolean ={
    getBoolParam("HashingTF.binary")
  }
  def setNumFeatures(num:Int):this.type={
    assert(num>0)
    addParam("HashingTF.numFeatures",num)
  }

  override def getPluginName: String = "TF词频分析"
}
