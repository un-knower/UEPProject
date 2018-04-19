package com.haiyisoft.bds.plugins.process.complex.util

import org.apache.spark.sql.{DataFrame, Row}

import scala.reflect.ClassTag

/**
  * Created by Namhwik on 2018/3/17.
  */
trait StaticsMethod {
  /**
    * 获取数值类型列的数据
    *
    * @param row        Row
    * @param columnName 列名
    * @param columnType 数据的SchemaMap
    * @return
    */
  def getRowValue(row: Row, columnName: String, columnType: String): Either[Double, String] = {
    columnType match {
      case "integer" => Left(row.getAs[Int](columnName).toDouble)
      case "double" => Left(row.getAs[Double](columnName))
      case "long" => Left(row.getAs[Long](columnName).toDouble)
      case "decimal" => Left(row.getAs[java.math.BigDecimal](columnName).doubleValue())
      case "string" => Left(row.getAs[String](columnName).toDouble)
      case _ => Right(row.getAs[String](columnName))
    }
  }

  /**
    * 获取数据的SchemaMap
    *
    * @param dataframe DataFrame
    * @return
    */
  def getColumnType(dataframe: DataFrame): Map[String, String] = {
    dataframe.schema.map(x => {
      val typeName = if (x.dataType.typeName.startsWith("decimal")) "decimal" else x.dataType.typeName
      (x.name, typeName)
    }).toMap
  }

  /**
    * 数组相加
    *
    * @param arr0 Array[T]
    * @param arr1 Array[T]
    * @tparam T ClassTag
    * @return
    */
  def reducer[T: ClassTag](arr0: Array[T], arr1: Array[T], operationsTypes: Array[String]): Array[Double] = {
    if (arr0.length == arr1.length && arr0.length == operationsTypes.length) {
      operationsTypes.indices.map(
        index => {
          val a = arr0(index).toString.toDouble
          val b = arr1(index).toString.toDouble
          operationsTypes(index).toUpperCase match {
            case "COUNT" => a + b
            case "SUM" => a + b
            case "MAX" => if (a > b) a else b
            case "MIN" => if (a > b) b else a
          }
        }
      ).toArray
    }
    else
      throw new RuntimeException("reducer arr length can not match ...")
  }

  /**
    * 获取分类类型列的数据
    *
    * @param row           Row
    * @param columnName    列名
    * @param columnTypeMap 数据的SchemaMap
    * @return
    */
  def getDiscretedRowValue(row: Row, columnName: String, columnTypeMap: Map[String, String]): String = {
    columnTypeMap(columnName) match {
      case "string" => row.getAs[String](columnName)
      case "integer" => row.getAs[Int](columnName).toString
      case "double" => row.getAs[Double](columnName).toString
      case "long" => row.getAs[Long](columnName).toDouble.toString
      case "decimal" => row.getAs[java.math.BigDecimal](columnName).toString
    }
  }
  /**
    * 数组相加
    *
    * @param arr0
    * @param arr1
    * @tparam T
    * @return
    */
  def reducer[T: ClassTag](arr0: Array[T], arr1: Array[T]): Array[Double] = {
    arr0.indices.map(index => arr0(index).toString.toDouble + arr1(index).toString.toDouble).toArray
  }


  /**
    * 二维数组相加
    *
    * @param arr0
    * @param arr1
    * @tparam T
    * @return
    */
  def reducer2[T: ClassTag](arr0: Array[Array[T]], arr1: Array[Array[T]]): Array[Array[Double]] = {
    arr0.indices.map(index => reducer(arr0(index), arr1(index))).toArray
  }
}
