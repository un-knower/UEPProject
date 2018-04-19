package com.haiyisoft.bds.plugins.process.ml.feature

import com.haiyisoft.bds.api.data.trans.DataProcessInter
import com.haiyisoft.bds.api.param.ParamFromUser
import com.haiyisoft.bds.api.param.col.{HasOneInputParamByName, HasOutputParam}
import org.apache.spark.ml.feature.{Bucketizer => MLTransformer}
import org.apache.spark.sql.DataFrame

/**
  * Created by XingxueDU on 2018/3/21.
  */
class Bucketizer extends DataProcessInter with HasOneInputParamByName with HasOutputParam {

  override def transform(data: DataFrame): DataFrame = {
    val inputCol = getInputColsImpl
    val outputCol = getOutputCol
    val indexer = new MLTransformer()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setSplits(getSplits)
    indexer.transform(data)
  }

  override def getNecessaryParamList: Array[ParamFromUser] = {
    val inputCol = oneInputColFromUser("String")
    val outputCol = outputColFromUser("Double")

    val Splits = ParamFromUser.Double("Splits","请输入切分点#Split points for mapping continuous features into buckets. With n+1 splits, there are n " +
      "buckets. A bucket defined by splits x,y holds values in the range [x,y) except the last " +
      "bucket, which also includes y. The splits should be of length >= 3 and strictly " +
      "increasing. Values at -inf, inf will be automatically provided to cover all Double values; "
    ).setAllowMultipleResult(true).addDefaultValue("0.0")

    Array(inputCol,outputCol,Splits)
  }

  def getSplits:Array[Double]={
    val res = getArrayDoubleParam("Splits")
    val res1 = res++Array(Double.NegativeInfinity,Double.PositiveInfinity)
    val res2 = res1.sorted
    res2
  }

  /*
  def checkSplits(splits: Array[Double]): Boolean = {
    if (splits.length < 3) {
      false
    } else {
      var i = 0
      val n = splits.length - 1
      while (i < n) {
        if (splits(i) >= splits(i + 1)) return false
        i += 1
      }
      true
    }
    */
  override def getPluginName: String = "Bucketizer分箱"
}
