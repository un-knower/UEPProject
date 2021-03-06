package com.haiyisoft.bds.api.param.col

import com.haiyisoft.bds.api.param.{ParamFromUser, WithParam}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by XingxueDU on 2018/3/22.
  */
trait HasOutputParam extends WithParam {
  final val OUTPUT_COL_KEY = ParamFromUser.variables.outputColName

  def getOutputCol: String = {
    val res = $(OUTPUT_COL_KEY)
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
    addParam(OUTPUT_COL_KEY, name)
    this
  }

  protected override def getDefaultParamList: ArrayBuffer[ParamFromUser] = {
    val s = super.getDefaultParamList
    s.+=(outputColFromUser())
    s
  }

  protected def outputColFromUser(outType: String = null): ParamFromUser = {
    val s = if (outType != null) s",输出列的格式将为$outType" else ""
    ParamFromUser.String(ParamFromUser.variables.outputColName, s"请输入结果列列名#outputColName" + s, notEmpty = true)
      .setOutputTypeName()
      .setShortName("输出列")
  }
}
