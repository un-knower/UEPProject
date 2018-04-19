package com.haiyisoft.bds.plugins.loader

import com.haiyisoft.bds.api.data.DataLoadInter
import com.haiyisoft.bds.api.param.ParamFromUser
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by DU Xingxue on 2018/3/20.
  */
class ParquetDataLoader extends DataLoadInter {

  override def getData: DataFrame = {
    SparkSession.builder().getOrCreate().read.parquet(getLoadPath)
  }

  override def getNecessaryParamList: Array[ParamFromUser] = {
    Array(ParamFromUser.save.loadPath().setShortName("加载路径"))
  }

  override def getPluginName: String = "从parquet读取"
}
