package com.haiyisoft.bds.plugins.process.udf

import com.haiyisoft.bds.api.data.trans.Udf2DataMining
import com.haiyisoft.bds.api.param.spark.HasMapStringBroadcast
import org.apache.spark.sql.functions._

/**
  * Created by XingxueDU on 2018/3/27.
  */
class DickFilter extends Udf2DataMining() with HasMapStringBroadcast{

  override def getNumBroadcast: Int = 1

  override def getNecessaryParamList = super.getNecessaryParamList:+{broadcastFromUser().setDescription("请输入字典项广播变量")}

  override protected def udfInputColNum: Int = 1

  override def initialize(): DickFilter.this.type = {
    val myudf = udf((col: String) => {
      val array = getMapStringBroadcast().value
      if (array.contains(col)) col else "null"
    })
    this.myudf = myudf
    this
  }
}