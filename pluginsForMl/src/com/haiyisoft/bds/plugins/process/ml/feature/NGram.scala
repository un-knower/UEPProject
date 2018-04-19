package com.haiyisoft.bds.plugins.process.ml.feature

import com.haiyisoft.bds.api.data.trans.DataProcessInter
import com.haiyisoft.bds.api.param.ParamFromUser
import com.haiyisoft.bds.api.param.col.{HasOneInputParamByName, HasOutputParam}
import org.apache.spark.ml.feature.{NGram => MLTraining}
import org.apache.spark.sql.DataFrame

/**
  * Created by XingxueDU on 2018/3/23.
  */
class NGram extends DataProcessInter with HasOneInputParamByName with HasOutputParam{
  private val training = new MLTraining()

  /**
    * 变换的执行接口，从待变换的data转化为变换后的data
    *
    * @param data 待变换的表
    * @return 变换后的表
    */
  override def transform(data: DataFrame): DataFrame = {
    training.setN(getN)
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
    val n = n_ParamFromUser
    Array(input,output,n)
  }

  ///////////////
  ////参数N
  //////////////
  val N_PARAM_KEY = "n"
  def n_ParamFromUser:ParamFromUser = {
    val res = ParamFromUser.Integer("n","请输入N值#number elements per n-gram (>=1)")>=1
    res.fix.addDefaultValue("2")
  }
  def getN:Int = getValueParam(N_PARAM_KEY).intValue()
  def setN(n:Int):this.type ={
    assert(n>=1)
    addParam(N_PARAM_KEY,n)
  }

  override def getPluginName: String = "N元模型"
}
