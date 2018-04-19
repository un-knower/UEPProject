package com.haiyisoft.bds.api.param.col

import com.haiyisoft.bds.api.param.ParamFromUser
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by XingxueDU on 2018/3/22.
  */
trait HasMultipleInputParamByColumn extends HasInputParam {

  override def setInputCols(name: String, otherName: String*): this.type = {
    val in = (name +: otherName).map(col)
    setInputCols(in: _*)
    this
  }

  override def setInputCols(cols: Column*): this.type = {
    addParam(INPUT_COL_KEY, cols.toArray)
  }

  override def getInputCols: Array[Column] = {
    getInputColsImpl
  }

  def getInputColsImpl: Array[Column] = {
    val res = getParam(INPUT_COL_KEY)
    println(res)
    if (res == null) {
      throw new IllegalArgumentException("should set InputCol")
    }
    res match {
      case c: Array[_] =>
        if (c.isEmpty) Array[Column]()
        else {
          c.head match {
            case y: Column => c.map(_.asInstanceOf[Column])
            case _ => c.map(k => col(k.toString))
          }
        }
      case _ =>
        getArrayStringParam(INPUT_COL_KEY).map(k => col(k.toString))
    }
  }

  protected override def getDefaultParamList: ArrayBuffer[ParamFromUser] = {
    val s = super.getDefaultParamList
    s.+=(multipleInputColFromUser())
    s
  }

  //TODO 也许需要针对特殊列（abs之类）进行处理？
  protected def multipleInputColFromUser(inType: String = null): ParamFromUser = {
    val t = if (inType != null) s",所有列列数据类型应该为$inType" else ""
    ParamFromUser.String(ParamFromUser.variables.inputColName, s"请输入传入列列名#inputColNames$t", notEmpty = true)
      .setAllowMultipleResult(true)
      .setInputTypeName()
      .setShortName("输入列")
  }
}
