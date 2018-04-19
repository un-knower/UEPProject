package com.haiyisoft.bds.plugins.process.complex

import com.haiyisoft.bds.api.data.trans.DataProcessInter
import com.haiyisoft.bds.api.param.ParamFromUser
import com.haiyisoft.bds.api.param.col.HasOutputParam
import com.haiyisoft.bds.plugins.util.SparkUtils._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
/**
  * Created by xujing on 2018/3/17.
  */
class Row2Col extends DataProcessInter with HasOutputParam{
  var keyColumns: Array[String] = _
  var valueColumn: String = _

  override def transform(dataFrame: DataFrame): DataFrame = {
    keyColumns = getArrayStringParam("REDUCE_KEYS")
    valueColumn = getStringParam("REDUCE_VALUES")
    if (keyColumns.nonEmpty && valueColumn.nonEmpty) {
      val ctMap = getColumnType(dataFrame)
      val allColumns = (keyColumns ++ Array(valueColumn)).map(_.toString)
      val schema = StructType(
        allColumns.map(k => StructField(s"$k", StringType))
      )

      //import dataFrame.sparkSession.implicits._
      val tmpRdd=dataFrame.rdd.map(row =>
          (keyColumns.map(keyColumn => getDiscretedRowValue(row, keyColumn, ctMap)).mkString("|"),
            getKeyValue(row, valueColumn, ctMap(valueColumn)))
      ).reduceByKey((a,b)=>a+","+b).map { x =>
        //val featureArray = x._2.split(",").map(_.toDouble)
        //val featureVector = Vectors.dense(featureArray)
       // val allValues = (x._1.split("\\|")++Array(featureVector))
        //val valueSeq =  (allValues.indices.map(s=>allValues(s)))
        val allValues = x._1.split("\\|") ++ Array(x._2)
        Row(allValues.indices.map(s => allValues(s)): _*)
      }//.toDF(allColumns.indices.map(s=>keyColumns(s)):_*)  //indices转换成sequence

      import org.apache.spark.sql.functions._
      val rtcudf = udf{(input:String)=>
        val inputArray = input.split(",").map(_.toDouble)
        Vectors.dense(inputArray)
      }
      val rtc_df=dataFrame.sparkSession.createDataFrame(tmpRdd,schema).
        withColumn(getOutputCol,rtcudf(col(valueColumn))).drop(valueColumn)
      rtc_df
    }
    else
      dataFrame
  }

  private def getKeyValue(row: Row, columnName: String, schemaType: String): String = {
    val v = getRowValue(row, columnName, schemaType)
    if (v.isLeft)
      v.left.get.toString
    else
      throw new RuntimeException(s"$columnName IS $schemaType ,DO NOT SUPPORT $schemaType ... ")

  }


  override def getNecessaryParamList: Array[ParamFromUser] = {
    val key = ParamFromUser.String("REDUCE_KEYS").setAllowMultipleResult(true)
    val value = ParamFromUser.String("REDUCE_VALUES")
    val out = outputColFromUser("ml.Vector")
    Array(key,value,out)
  }

  override def getPluginName: String = "Row to Col"
}
