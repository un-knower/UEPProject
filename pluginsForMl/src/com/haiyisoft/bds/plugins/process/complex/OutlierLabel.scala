package com.haiyisoft.bds.plugins.process.complex

import com.haiyisoft.bds.api.data.trans.DataProcessInter
import com.haiyisoft.bds.api.param.ParamFromUser
import com.haiyisoft.bds.api.param.col.{HasOneInputParamByName, HasOutputParam}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

/**
  * Created by xujing on 2018/3/19.
  */
class OutlierLabel extends DataProcessInter with HasOutputParam with HasOneInputParamByName {
  final val METHOD_TYPE = "METHOD"
  final val RELATIVE_COL = "RELATIVECOL"
  final val RELATIVE_ERROR = "RELATIVEERROR"

  override def transform(data: Dataset[Row]): Dataset[Row] = {
    val columns = getInputCols
    val outCol = getOutputCol
    val method_type = getStringParam(METHOD_TYPE).toString
    val relative_col = getStringParam(RELATIVE_COL) //.asInstanceOf[String]
    val relativeError = getParam(RELATIVE_ERROR).toString.toDouble
    //    println("=========="+relative_col)
    //根据有没有相关联的列，区分有几个类的阈值
    if ("ALL" != relative_col) {

      def concurrentHashMap[K, V]() = {
        import scala.collection.convert.decorateAsScala._
        val map: scala.collection.concurrent.Map[K, V] = new java.util.concurrent.ConcurrentHashMap[K, V]().asScala
        map
      }

      val thresholdMap = concurrentHashMap[String, Array[Double]]()
      val relativeValueArray = data.select(relative_col).distinct().rdd.map(x => x.get(0).toString).collect()


      relativeValueArray.par.foreach { x =>
        val data_filter = data.filter(relative_col + "='" + x + "'").select(columns(0)) //.rdd.map(x=>x.get(0).toString.toDouble).collect()
        thresholdMap.put(x, get_threshold(method_type, data_filter, relativeError))
      }

      val get_label1 = udf { (col1: String, col2: String) =>
        val threshold_low = thresholdMap(col2)(0)
        val threshold_high = thresholdMap(col2)(1)
        if (col1.toDouble >= threshold_high || col1.toDouble <= threshold_low) {
          "1"
        }
        else {
          "0"
        }
      }
      data.withColumn(outCol, get_label1(columns(0), col(relative_col)))
    } else {
      val thresholdArray: Array[Array[Double]] = new Array(1)
      val data_filter = data.select(columns(0)) //.rdd.map(x=>x.get(0).toString.toDouble).collect()
      thresholdArray(0) = get_threshold(method_type, data_filter, relativeError)

      val get_label2 = udf { (col1: String) =>
        val threshold_low = thresholdArray(0)(0)
        val threshold_high = thresholdArray(0)(1)
        if (col1.toDouble >= threshold_high || col1.toDouble <= threshold_low) {
          "1"
        }
        else {
          "0"
        }
      }
      data.withColumn(outCol, get_label2(columns(0)))
    }

  }

  def get_threshold(method_type: String, data_filter: DataFrame, relativeError: Double = 0.0): Array[Double] = {
    method_type.toUpperCase match {
      case "STD" =>
        val summaryratedf = data_filter.describe()
        val summaryrateArray = summaryratedf.rdd.map { r =>
          r.get(1).toString.toDouble
        }.collect()
        val ratemean = summaryrateArray(1)
        val ratestd = summaryrateArray(2)
        val dfLow = ratemean - 2.3 * ratestd
        val dfHigh = ratemean + 2.3 * ratestd
        Array(dfLow, dfHigh)
      case "QRT" =>
        val column = data_filter.columns.head
        val lowerUpper = data_filter.stat.approxQuantile(column, Array(0.25, 0.75), 1 - relativeError)
        Array(lowerUpper(0) - 1.5 * (lowerUpper(1) - lowerUpper(0)), lowerUpper(0) + 1.5 * (lowerUpper(1) - lowerUpper(0)))

      case _ => throw new RuntimeException("Don't support other methods except STD and QRT")
    }
  }

  override def getNecessaryParamList: Array[ParamFromUser] = {
    val input = oneInputColFromUser()
      .setDescription("请输入对离群判断的所根据的列名")
      .setShortName("输入列")
    val output = outputColFromUser().setDescription("请输入输出列")
      .setShortName("输出列")
    val RELATIVECOL = ParamFromUser
      .String(RELATIVE_COL, "请输入分类列名或者保留ALL", notEmpty = true)
      .setShortName("分类列")
      .addDefaultValue("ALL")
    val METHOD = ParamFromUser
      .SelectBox(METHOD_TYPE, "请选择方法#请选择方法", "STD", "QRT")
      .setShortName("离群算法")
    val RelativeError = ParamFromUser
      .Double(RELATIVE_ERROR, "输入计算精度0~1，最高为1(计算全部数据)")
      .addDefaultValue("0.45")
      .setShortName("计算精度")

    Array(input, output, RELATIVECOL, METHOD, RelativeError)
  }

  override def getPluginName: String = "Outlier Label"
}