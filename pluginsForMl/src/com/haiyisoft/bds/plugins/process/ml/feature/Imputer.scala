package com.haiyisoft.bds.plugins.process.ml.feature

import com.haiyisoft.bds.api.data.trans.DataProcessInter
import com.haiyisoft.bds.api.param.ParamFromUser
import com.haiyisoft.bds.api.param.col.{HasMultipleOutputParam, HasMultipleInputParamByName}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

//TODO 等待测试
/**
  * 以尽量使更正对数据集影响最小的方式填充缺失数据
  * Created by XingxueDU on 2018/3/23.
  *
  */
class Imputer extends DataProcessInter with HasMultipleInputParamByName with HasMultipleOutputParam{
  addParam("missingValue","Double.NaN")
  /**
    * 变换的执行接口，从待变换的data转化为变换后的data
    *
    * @param dataset 待变换的表
    * @return 变换后的表
    */
  override def transform(dataset: DataFrame): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    val inputCols = getInputColsImpl
    val cols = inputCols.map { inputCol =>
      when(col(inputCol).equalTo(Double.NaN),null)
        .when(col(inputCol).isNaN, null)
        .otherwise(col(inputCol))
        .cast("double")
        .as(inputCol)
    }
    val results:Array[Double] = getStrategy match {
      case "mean" =>
        // Function avg will ignore null automatically.
        // For a column only containing null, avg will return null.
        val row = dataset.select(cols.map(avg): _*).head()
        Array.range(0, inputCols.length).map { i =>
          if (row.isNullAt(i)) {
            Double.NaN
          } else {
            row.getDouble(i)
          }
        }
      case "median" =>
        // Function approxQuantile will ignore null automatically.
        // For a column only containing null, approxQuantile will return an empty array.
        inputCols.indices.map{idx=>
          val array = dataset.select(cols(idx)).stat.approxQuantile(inputCols(idx), Array(0.5), 0.001)
          if (array.isEmpty) {
            Double.NaN
          } else {
            array.head
          }
        }.toArray
    }
    val emptyCols = inputCols.zip(results).filter(_._2.isNaN).map(_._1)
    if (emptyCols.nonEmpty) {
      throw new Exception(s"surrogate cannot be computed. " +
        s"All the values in ${emptyCols.mkString(",")} are Null, Nan or " +
        s"missingValue(${getStringParam("missingValue")})")
    }
    val rows = spark.sparkContext.parallelize(Seq(Row.fromSeq(results)))
    val schema = StructType(inputCols.map(col => StructField(col, DoubleType, nullable = false)))
    val surrogateDF = spark.createDataFrame(rows, schema)
    val surrogates = surrogateDF.select(inputCols.map(col): _*).head().toSeq
    val outputCols = getOutputCol
    val newCols = inputCols.zip(outputCols).zip(surrogates).map {
      case ((inputCol, outputCol), surrogate) =>
        val inputType = dataset.schema(inputCol).dataType
        val ic = col(inputCol)
        when(ic.isNull, surrogate)
          .when(ic === Double.NaN, surrogate)
          .otherwise(ic)
          .cast(inputType)
    }
    (dataset/: outputCols.indices){case(df,idx)=>
      df.withColumn(outputCols(idx),newCols(idx))
    }
  }
  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  override def getNecessaryParamList: Array[ParamFromUser] = {
    val method = ParamFromUser.SelectBox("strategy","请选择以mean平均数还是median中位数来补位异常值","mean","median")
    val input = multipleInputColFromUser("Double")
    val output = outputColFromUser("Double")
    Array(input,output,method)
  }
  def setStrategy(s:String):this.type ={
    assert(s=="mean"||s=="median")
    addParam("strategy",s)
  }
  def getStrategy:String = getStringParam("strategy")

  override def getPluginName: String = "缺失数据填充"
}