package com.haiyisoft.bds.api.param.col

import com.haiyisoft.bds.api.param.ParamFromUser
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by XingxueDU on 2018/3/22.
  */
trait HasOneInputParamByName extends HasInputParam {
  override def setInputCols(name: String, otherName: String*): this.type = {
    if (otherName.nonEmpty) throw new IllegalArgumentException("Allow Only One Input Column")
    addParam(INPUT_COL_KEY, name)
    this
  }

  override def setInputCols(cols: Column*): this.type = {
    if (cols.length != 1) throw new IllegalArgumentException("Allow Only One Input Column")
    setInputCols(cols.head.toString())
  }

  override def getInputCols: Array[Column] = {
    Array(col(getInputColsImpl))
  }

  def getInputColsImpl: String = {
    val res = $(INPUT_COL_KEY)
    if (res == null) {
      throw new IllegalArgumentException("should set InputCol")
    }
    res
  }

  protected override def getDefaultParamList: ArrayBuffer[ParamFromUser] = {
    val s = super.getDefaultParamList
    s.+=(oneInputColFromUser())
    s
  }

  protected def oneInputColFromUser(inType: String = null): ParamFromUser = {
    val s = if (inType != null) s",该列数据类型应该为$inType" else ""
    ParamFromUser
      .String(ParamFromUser.variables.inputColName, s"请输入传入列列名#inputColName" + s, notEmpty = true)
      .setNeedSchema(true)
      .setInputTypeName()
      .setShortName("输入列")
  }
}
