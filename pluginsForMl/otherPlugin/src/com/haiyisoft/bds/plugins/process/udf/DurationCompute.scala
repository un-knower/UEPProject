package com.haiyisoft.bds.plugins.process.udf

import java.sql.Timestamp

import com.haiyisoft.bds.api.data.trans.Udf2DataMining
import org.apache.spark.sql.functions._

/**
  * Created by Namhwik on 2018/4/2.
  */
class DurationCompute extends Udf2DataMining {

  override protected def udfInputColNum: Int = 1

  override def initialize(): DurationCompute.this.type = {
    val durationCompute = udf((t1: Timestamp) =>
      math.abs(t1.getTime - System.currentTimeMillis()) / ((1000 * 60 * 60 * 24).toDouble * 30.00 * 12)

    )
    this.myudf = durationCompute
    this
  }
}
