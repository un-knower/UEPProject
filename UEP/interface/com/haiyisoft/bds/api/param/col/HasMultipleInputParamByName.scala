package com.haiyisoft.bds.api.param.col

import com.haiyisoft.bds.api.param.ParamFromUser
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by XingxueDU on 2018/3/22.
  */
trait HasMultipleInputParamByName extends HasInputParam {

  override def setInputCols(name: String, otherName: String*): this.type = {
    addParam(INPUT_COL_KEY, (name +: otherName).mkString(SYMBOL_SEPARATOR.toString))
    this
  }

  override def setInputCols(cols: Column*): this.type = {
    val s = cols.map(_.toString)
    setInputCols(s.head, s.tail: _*)
  }

  override def getInputCols: Array[Column] = {
    getInputColsImpl.map(col)
  }

  def getInputColsImpl: Array[String] = {
    val res = $(INPUT_COL_KEY)
    if (res == null) {
      throw new IllegalArgumentException("should set InputCol and OutputCol")
    }
    res.split(SYMBOL_SEPARATOR)
  }

  protected override def getDefaultParamList: ArrayBuffer[ParamFromUser] = {
    val s = super.getDefaultParamList
    s.+=(multipleInputColFromUser())
    s
  }

  protected def multipleInputColFromUser(inType: String = null, separator: Char = '@'): ParamFromUser = {
    val s = if (separator != '@') s",以${separator}分隔" else ""
    val t = if (inType != null) s",所有列列数据类型应该为$inType" else ""
    ParamFromUser.String(ParamFromUser.variables.inputColName, s"请输入传入列列名#inputColNames$s$t", true)
      .setAllowMultipleResult(true)
      .setNeedSchema(true)
      .setInputTypeName()
      .setShortName("输入列")
  }
}
