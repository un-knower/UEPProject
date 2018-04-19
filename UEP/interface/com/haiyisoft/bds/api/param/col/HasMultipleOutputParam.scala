package com.haiyisoft.bds.api.param.col

import com.haiyisoft.bds.api.param.{ParamFromUser, WithParam}

import scala.collection.mutable.ArrayBuffer

/**
  * 算法会生成复数列结果
  * 该特性与[[HasOutputParam]]不兼容，根据加载顺序不同只会体现出二者之一
  * Created by XingxueDU on 2018/3/22.
  */
trait HasMultipleOutputParam extends WithParam {
  final val OUTPUT_COL_KEY = ParamFromUser.variables.outputColName

  def getOutputCol: Array[String] = {
    val res = getArrayStringParam(OUTPUT_COL_KEY)
    if (res == null) {
      throw new IllegalArgumentException("should set InputCol and OutputCol")
    }
    res
  }

  /**
    * 设置输出列
    *
    * @param name   输出列的列名
    * @param others 如果算法输出复数列则由others承接。否则可以无视此参数
    */
  def setOutputCol(name: String, others: String*): this.type = {
    val res = name +: others
    addParam(OUTPUT_COL_KEY,
      multiParam2Str(res:_*)
      //res.mkString(ParamFromUser.variables.separator.toString)
    )
    this
  }

  protected override def getDefaultParamList: ArrayBuffer[ParamFromUser] = {
    val s = super.getDefaultParamList
    s.+=(outputColFromUser())
    s
  }

  protected def outputColFromUser(outType: String = null): ParamFromUser = {
    val s = if (outType != null) s",输出列的格式将为$outType" else ""
    ParamFromUser
      .String(ParamFromUser.variables.outputColName, s"请输入结果列列名#outputColName" + s, notEmpty = true).setAllowMultipleResult(true)
      .setOutputTypeName()
      .setShortName("输出列")
  }
}
