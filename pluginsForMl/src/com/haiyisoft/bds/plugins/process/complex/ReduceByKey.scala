package com.haiyisoft.bds.plugins.process.complex

import com.haiyisoft.bds.api.data.{Converter, DataTransformInter}
import com.haiyisoft.bds.api.param.ParamFromUser
import com.haiyisoft.bds.plugins.util.SparkUtils._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by Namhwik on 2018/3/15.
  */
class ReduceByKey extends DataTransformInter {
  var keyColumns: Array[String] = _
  var operations: Array[(String, String)] = _

  override def transform(dataFrame: DataFrame): DataFrame = {
    keyColumns = getArrayStringParam("REDUCE_KEYS")
    operations = getArrayStringParam("OPREATIONS").map { k =>
      val s = k.split(',')
      (s(0), s(1))
    }
    if (keyColumns == null || operations == null)
      throw new RuntimeException("Param must be init ...")
    if (keyColumns.nonEmpty && operations.nonEmpty) {
      val ctMap = getColumnType(dataFrame)

      val operationTypes = operations.map(_._2)
      val operationsArr = operations
      val schema = StructType(
        keyColumns.map(k => StructField(s"$k", StringType)) ++
          operationsArr.indices
            .map(i => StructField(s"${operationsArr(i)._1}_${operationTypes(i)}", DoubleType))
      )
      schema.foreach(println)
      val rdd = dataFrame.rdd.map(
        row =>
          (keyColumns.map(keyColumn => getDiscretedRowValue(row, keyColumn, ctMap)).mkString("|"),
            operationsArr.indices.map(k => getKeyValue(row, operationsArr(k)._1, operationsArr(k)._2, ctMap(operationsArr(k)._1))).toArray)
      ).reduceByKey((a, b) => reducer(a, b, operationTypes))
        .map(x => {
          val values = x._1.split("\\|") ++ x._2
          Row(values.indices.map(x => if (x < keyColumns.length) values(x).toString else values(x).toString.toDouble): _*)
        })
      rdd.take(20).foreach(println(_))
      dataFrame.sparkSession.createDataFrame(rdd, schema)
    }
    else
      dataFrame
  }

  private def getKeyValue(row: Row, columnName: String, reduceType: String, schemaType: String): Double = {
    val reduceTypeUpper = reduceType.toUpperCase
    reduceTypeUpper match {
      case "COUNT" => 1
      case "SUM" =>
        val v = getRowValue(row, columnName, schemaType)
        if (v.isLeft)
          v.left.get
        else
          throw new RuntimeException(s"$columnName IS $schemaType ,DO NOT SUPPORT $reduceTypeUpper ... ")
      case "MAX" =>
        val v = getRowValue(row, columnName, schemaType)
        if (v.isLeft)
          v.left.get
        else
          throw new RuntimeException(s"$columnName IS $schemaType ,DO NOT SUPPORT $reduceTypeUpper ... ")
      case "MIN" =>
        val v = getRowValue(row, columnName, schemaType)
        if (v.isLeft)
          v.left.get
        else
          throw new RuntimeException(s"$columnName IS $schemaType ,DO NOT SUPPORT $reduceTypeUpper ... ")
    }
  }

  override def getNecessaryParamList: Array[ParamFromUser] = {
    val keys = ParamFromUser.String("REDUCE_KEYS")
      .setAllowMultipleResult(true)
      .setShortName("聚合列")
      .setNeedSchema(true)
    val rule = "function check(a){return a.split(\",\").length==2;}check(\"#value#\")"
    val ops = ParamFromUser("OPREATIONS", rule = rule)
      .setAllowMultipleResult(true)
      .setTypeName("String, String")
      .setShortName("聚合操作")
    Array(keys, ops)
  }

  override def getPluginName: String = "ReduceByKey"

  override def getTransType: Converter.Value = Converter.SHUFFLE

}
