package com.haiyisoft.bds.plugins.process.udf

import com.haiyisoft.bds.api.data.trans.Udf2DataMining

/**
  * Created by Namhwik on 2018/4/4.
  */
class String2Double extends Udf2DataMining {

  override def getPluginName: String = "数字类型转换"

  override protected def udfInputColNum: Int = 1

  override def initialize(): String2Double.this.type = {
    import org.apache.spark.sql.functions.udf
    val str2Double = udf((str: String) => str.toDouble)
    this.myudf = str2Double
    this
  }


}
