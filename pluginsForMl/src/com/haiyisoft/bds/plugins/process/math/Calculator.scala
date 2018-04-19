package com.haiyisoft.bds.plugins.process.math

import com.haiyisoft.bds.api.data.trans.DataProcessInter
import com.haiyisoft.bds.api.param.ParamFromUser
import com.haiyisoft.bds.api.param.col.{HasOutputParam, HasMultipleInputParamByColumn}
import com.haiyisoft.bds.plugins.util.SparkUtils
import org.apache.spark.sql.DataFrame

/**
  * Created by XingxueDU on 2018/3/27.
  */
class Calculator extends DataProcessInter with HasMultipleInputParamByColumn with HasOutputParam {
  final val OPERATOR_KEY = "math.operator"

  /**
    * 变换的执行接口，从待变换的data转化为变换后的data
    *
    * @param data 待变换的表
    * @return 变换后的表
    */
  override def transform(data: DataFrame): DataFrame = {
    val input = getInputColsImpl
    val output = getOutputCol
    val op = getOperator
    val length = input.length
    val myUdf = SparkUtils.LengthChangeableUdf[Object, Double] { seqO =>
      op(seqO.map(k => BigDecimal(k.toString)).toArray)
    }(length)
    data.withColumn(output, myUdf(input: _*))
  }

  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  override def getNecessaryParamList: Array[ParamFromUser] = {
    val input = multipleInputColFromUser("Double")
      .setNeedSchema(true)
      .setShortName("输入列")
    val output = outputColFromUser("Double").setShortName("输出列")
    val op = ParamFromUser.SelectBox(OPERATOR_KEY, "请选择运算符#运算符",
      "加法(sum)", "减法(a-b-c...)", "乘法(multiple)", "除法(a/(b*c*d))",
      "平方和(a²+b²+...)", "和的平方(a+b+...)²", "开根号√(a+b+...)", "绝对值(|a|+|b|+...)")
      .setShortName("运算符")
    Array(input, output, op)
  }

  def setOperator(op: String): this.type = {
    addParam(OPERATOR_KEY, op)
  }

  def getOperator: Array[BigDecimal] => Double = {
    getStringParam(OPERATOR_KEY) match {
      case "加法(sum)" =>
        (in: Array[BigDecimal]) => in.sum.doubleValue()
      case "减法(a-b-c...)" =>
        (in: Array[BigDecimal]) => (in.head /: in.tail) (_ - _).doubleValue()
      case "乘法(multiple)" =>
        (in: Array[BigDecimal]) => (in.head /: in.tail) (_ * _).doubleValue()
      case "除法(a/(b*c*d))" =>
        (in: Array[BigDecimal]) => (in.head /: in.tail) (_ / _).doubleValue()
      case "平方和(a²+b²+...)" =>
        (in: Array[BigDecimal]) => in.map(k => k * k).sum.doubleValue()
      case "和的平方(a+b+...)²" =>
        (in: Array[BigDecimal]) => {
          val s = in.sum
          s * s doubleValue()
        }
      case "开根号√(a+b+...)" =>
        (in: Array[BigDecimal]) => {
          val s = in.sum.doubleValue()
          math.sqrt(s)
        }
      case "绝对值(|a|+|b|+...)" =>
        (in: Array[BigDecimal]) => in.map(k => if (k < 0) k * -1 else k).sum.doubleValue()
    }
  }

  override def getPluginName: String = "计算器"
}
