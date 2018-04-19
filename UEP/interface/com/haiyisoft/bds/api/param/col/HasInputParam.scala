package com.haiyisoft.bds.api.param.col

import com.haiyisoft.bds.api.param.{ParamFromUser, WithParam}

/**
  * Created by XingxueDU on 2018/3/22.
  */
trait HasInputParam extends WithParam{
  final val INPUT_COL_KEY = ParamFromUser.variables.inputColName
  /**
    * 以列名的方式传入列。
    *
    * @param name 首列名
    * @param otherName 其他列名
    */
  def setInputCols(name:String,otherName: String*): this.type

  /**
    * 用column的方式传入列，或者传入空列时实现的方法,通常用于withColumn及udf
    * 如果方法实际上不接受column则应该将此方法视为传入了空列
    *
    * @param cols col(name)*,see[[org.apache.spark.sql.functions.col()]]
    */
  def setInputCols(cols: org.apache.spark.sql.Column*): this.type

  def getInputCols: Array[org.apache.spark.sql.Column]

}
