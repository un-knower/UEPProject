package com.haiyisoft.bds.api.param.ml

import com.haiyisoft.bds.api.param.{ParamFromUser, WithParam}

/**
  * Param for regularization parameter (>= 0).
  */
trait HasRegParam extends WithParam{
  final val REG_PARAM_KEY = ParamFromUser.variables.regParam
  def setRegParam(regParam:Double):this.type ={
    addParam(REG_PARAM_KEY,regParam)
  }
  setRegParam(0d)

  def getRegParam:Double ={
    getValueParam(REG_PARAM_KEY).doubleValue()
  }
  protected def regParamFromUser:ParamFromUser={
    ParamFromUser.Double(REG_PARAM_KEY,"请输入正则项参数#正则项参数[范数阶位](>=0)，0不使用").>=(0).fix.addDefaultValue("0")
  }
}
