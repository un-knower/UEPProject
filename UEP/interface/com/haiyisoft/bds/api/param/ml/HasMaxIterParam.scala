package com.haiyisoft.bds.api.param.ml

import com.haiyisoft.bds.api.param.{ParamFromUser, WithParam}

/**
  * Param for maximum number of iterations (>= 0).
  *
  * Created by XingxueDU on 2018/3/22.
  */
trait HasMaxIterParam extends WithParam{
  final val MAX_ITER_KEY = ParamFromUser.variables.maxIter
  def setMaxIter(maxIter:Int):this.type={
    addParam(MAX_ITER_KEY,maxIter)
  }
  setMaxIter(100)

  def getMaxIter:Int = {
    getValueParam(MAX_ITER_KEY).intValue()
  }
  protected def maxIterFromUser:ParamFromUser = {
    ParamFromUser.Double(MAX_ITER_KEY,"请输入最大迭代次数#算法的最大迭代次数（>=0）").>=(0).fix.addDefaultValue("100")
  }
}
