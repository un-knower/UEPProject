package com.haiyisoft.bds.plugins.process.complex

import com.haiyisoft.bds.api.data.{Converter, DataTransformInter}
import com.haiyisoft.bds.api.param.ParamFromUser
import com.haiyisoft.bds.api.param.col.{HasMultipleInputParamByName, HasOutputParam}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable
import scala.reflect.ClassTag

/**
  * Created by Namhwik on 2018/3/17.
  *
  */

/**
  * 字典项统计
  *
  */
class DictionaryStat extends DataTransformInter with HasMultipleInputParamByName with HasOutputParam {
  //  final val necessaryParamList = Array("KeyCols", "DictionaryMap", "Operations")

  override def getPluginName: String = "字典项统计"

  val OPERATIONS_KEY = "Operations"
  val DICTIONARY_MAP_KEY = "DictionaryMap"

  override def getNecessaryParamList: Array[ParamFromUser] = {
    val input = multipleInputColFromUser().setDescription("请输入组成主键的列以进行reduce操作，可输入多个")
      .setNeedSchema(true)
      .setShortName("主键列")
    val output = outputColFromUser()
      .setDescription("请输入除了主键之外的输出列")
      .setShortName("输出列")
    val Operations = ParamFromUser.String(OPERATIONS_KEY, "输入聚合列、字典名及操作，用','分隔", notEmpty = true)
      .setAllowMultipleResult(true)
      .setNeedSchema(true)
      .setShortName("选择操作")
    val DictionaryMap = ParamFromUser.String(DICTIONARY_MAP_KEY, "输入字典项，按照'字典名|1,2,..@字典名|1,2'", notEmpty = true)
      .setAllowMultipleResult(true)
      .setNeedSchema(true)
      .setShortName("字典项")
    Array(input, output, Operations, DictionaryMap)
  }

  override def getTransType: Converter.Value = Converter.SHUFFLE

  def getOperations: Array[(String, String, String)] = {
    val res = getArrayStringParam(OPERATIONS_KEY)
    val res1 = res.flatMap { key =>
      val s = key.split(',')
      if (s.length == 3) Option((s.head, s(2), s(1)))
      else None
    }
    res1
  }

  def getDictionaryMap: Map[String, Array[String]] = {
    val res: Array[String] = getArrayStringParam(DICTIONARY_MAP_KEY)
    val res1 = res.flatMap { key =>
      val s = key.split('|')
      if (s.length == 2) {
        Option((s.head, s(1).split(',')))
      }
      else None
    }
    res1.toMap
  }

  override def transform(data: DataFrame): DataFrame = {

    val keyColumns = getInputColsImpl
    println(keyColumns.mkString("."))
    val dicMap = getDictionaryMap.map(x => (x._1, x._2))
    dicMap.foreach(x => println(x._1 + "=>" + x._2.mkString(",")))
    val outputs = getOutputCol.split('@')
    println("outputs" + outputs.mkString(","))

    val operations: Array[(String, String, String)] = getOperations
    operations.foreach(println(_))

    if (keyColumns.isEmpty || operations.isEmpty)
      throw new RuntimeException("参数不能为空")
    else {
      val schema = StructType(
        keyColumns.map(k => StructField(s"$k", StringType)) ++
          operations.indices
            .map(i => {
              operations(i)._2 match {
                case "MERGESUM" => StructField(s"${outputs(i)}", ArrayType(DoubleType))
                case "MERGECOUNT" => StructField(s"${outputs(i)}", ArrayType(DoubleType))
                case _ => StructField(s"${outputs(i)}", DoubleType)
              }

            }))

      val schemaMap = getColumnType(data)
      val rdd = data.rdd.mapPartitions(
        partitions => partitions.map {
          row => {
            (keyColumns.map(k => getDiscretedRowValue(row, k, schemaMap)).mkString("|")
              , operations.map(op => {
              val colType = schemaMap(op._1)
              val (dictionaryCol, dictionaryArr) = (op._3, dicMap.getOrElse(op._3, Array()))
              if (dictionaryArr.nonEmpty) {
                op._2.toUpperCase match {
                  case "MERGESUM" =>
                    dictionaryArr.map(a => {
                      val rowDicValue = getDiscretedRowValue(row, dictionaryCol, schemaMap)
                      if (a.equals(rowDicValue)) {
                        val rowValue = getRowValue(row, op._1, colType)
                        if (rowValue.isLeft)
                          rowValue.left.get
                        else
                          throw new RuntimeException(s"${op._1} is not numeric , do not support ${op._2}")
                      } else 0.0
                    })
                  case "MERGECOUNT" =>
                    dictionaryArr.map(a => {
                      val rowDicValue = getDiscretedRowValue(row, dictionaryCol, schemaMap)
                      if (a.equals(rowDicValue)) 1 else 0.0
                    })
                }
              }
              else {
                val rw = getRowValue(row, op._1, colType)


                op._2.toUpperCase match {


                  case "SUM" =>
                    if (rw.isLeft)
                      Array(rw.left.get)
                    else
                      throw new RuntimeException(s"${op._1} is not numeric , do not support ${op._2}")

                  case "COUNT" => Array(1.0)
                  case "MAX" =>
                    if (rw.isLeft)
                      Array(getRowValue(row, op._1, colType).left.get)
                    else
                      Array(getRowValue(row, op._1, colType).right.get.toDouble)

                  case "MIN" =>
                    if (rw.isLeft)
                      Array(getRowValue(row, op._1, colType).left.get)
                    else
                      Array(getRowValue(row, op._1, colType).right.get.toDouble)
                  case "MEDIAN" =>
                    if (rw.isLeft)
                      Array(getRowValue(row, op._1, colType).left.get)
                    else
                      Array(getRowValue(row, op._1, colType).right.get.toDouble)

                }
              }

            }))
          }
        }
      )

        .reduceByKey(
          (a, b) => reducer2(a, b, operations)
        )

        .map(
          x => {
            val rdRes = x._2

            Row(x._1.split("\\|") ++ operations.indices.map(
              i => {
                operations(i)._2 match {
                  case "MERGESUM" => rdRes(i)
                  case "MERGECOUNT" => rdRes(i)
                  case "MEDIAN" => getMedian(rdRes(i).toList)
                  case _ => rdRes(i).head
                }
              }
            ): _*)
          }
        )
      var res = data.sparkSession.createDataFrame(rdd, schema)
      res.show(false)
      import org.apache.spark.sql.functions._
      val array2Vector = udf((arr: mutable.WrappedArray[Double]) => Vectors.dense(arr.toArray))
      val mergeCols = operations.map(_._2).zipWithIndex.filter(p => p._1.startsWith("MERGE"))
      if (mergeCols.nonEmpty)
        mergeCols.map(_._2).foreach(x => res = res.withColumn(outputs(x), array2Vector(col(outputs(x)))))

      res
    }
  }


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
    * @return
    */
  def reducer2(arr0: Array[Array[Double]], arr1: Array[Array[Double]], ops: Array[(String, String, String)] = Array()): Array[Array[Double]] = {
    if (ops.isEmpty)
      arr0.indices.map(index => reducer(arr0(index), arr1(index))).toArray
    else
      ops.indices.map(
        i =>
          ops(i)._2 match {
            case "MEDIAN" =>
              arr0(i) ++ arr1(i)
            case "MAX" => Array((arr0(i) ++ arr1(i)).max)
            case "MIN" => Array((arr0(i) ++ arr1(i)).min)
            case _ => reducer(arr0(i), arr1(i))
          }
      ).toArray
  }

  /**
    * 计算一组数据的中位数
    *
    * @param l List[T]
    * @tparam T ClassTag
    * @return
    */
  def getMedian[T: ClassTag](l: List[T]): T = {
    val sortedArr = l.sortWith((a, b) => a.toString.toDouble > b.toString.toDouble).toArray
    sortedArr(l.length / 2)
  }
}
