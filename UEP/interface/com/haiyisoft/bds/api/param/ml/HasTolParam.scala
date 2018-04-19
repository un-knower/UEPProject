package com.haiyisoft.bds.api.param.ml

import com.haiyisoft.bds.api.param.{ParamFromUser, WithParam}

/**
  * Created by XingxueDU on 2018/3/22.
  */
trait HasTolParam extends WithParam{
  final val TOL_KEY = ParamFromUser.variables.tol
  def setTol(tol:Double):this.type={
    addParam(TOL_KEY,tol)
  }
  def getTol:Double={
    getValueParam(TOL_KEY).doubleValue()
  }
  protected def tolFromUser:ParamFromUser ={
    ParamFromUser.Double(TOL_KEY,"请输入收敛阈值#较低的收敛阈值可能在增加计算时间的同时增加准确度，但是也可能导致过拟合。").>=(0).fix.addDefaultValue("0.000001")
  }
  setTol(1E-6)
}
