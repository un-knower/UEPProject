package com.haiyisoft.bds.plugins.loader

import com.haiyisoft.bds.api.data.DataLoadInter
import com.haiyisoft.bds.api.param.ParamFromUser
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.{functions, SparkSession, DataFrame}

/**
  * Created by XingxueDU on 2018/3/22.
  */
class IRISLoader extends DataLoadInter{
  override def getData: DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    val string2double = functions.udf((in:String)=>in.toDouble)
    val df0 = spark.read.csv("./TestResource/iris.txt")
      .withColumn("x1",string2double(functions.col("_c0")))
      .withColumn("x2",string2double(functions.col("_c1")))
      .withColumn("x3",string2double(functions.col("_c2")))
      .withColumn("x4",string2double(functions.col("_c3")))
      .withColumnRenamed("_c4","labelString").select("labelString","x1","x2","x3","x4")

    df0.cache()
    df0.show
    df0
  }

  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  override def getNecessaryParamList: Array[ParamFromUser] = {
    Array()
  }

  override def getPluginName: String = "[DEBUG]鸢尾花数据集"
}
