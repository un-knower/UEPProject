package com.haiyisoft.bds.api.param.col

import com.haiyisoft.bds.api.param.{ParamFromUser, WithParam}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by XingxueDU on 2018/3/22.
  */
trait HasLabelColParam extends WithParam{
  final val LABEL_COL_KEY = ParamFromUser.variables.labelColName
  /**
    * 以列名的方式传入列。
    *
    * @param name 特征列列名
    */
  def setLabelCol(name:String): this.type={
    addParam(LABEL_COL_KEY,name)
    this
  }


  def getLabelCol: String= getStringParam(LABEL_COL_KEY)

  override protected def getDefaultParamList: ArrayBuffer[ParamFromUser]={
    val old = super.getDefaultParamList
    old+=labelColFromUser
    old
  }
  protected def labelColFromUser:ParamFromUser={
    ParamFromUser.String(ParamFromUser.variables.labelColName,s"请输入传入标签列列名#labelColName",true).addDefaultValue("label")
  }
}
