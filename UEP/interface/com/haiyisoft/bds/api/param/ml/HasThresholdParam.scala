package com.haiyisoft.bds.api.param.ml

import com.haiyisoft.bds.api.param.{WithParam, ParamFromUser}

/**
  * Created by XingxueDU on 2018/3/22.
  */
trait HasThresholdParam extends WithParam{
  final val THRESHOLD_KEY = ParamFromUser.variables.threshold
  /**
    * 设置分类阈值[0,1]。
    *
    * @param threshold 分类阈值
    */
  def setThreshold(threshold:Double): this.type={
    addParam(THRESHOLD_KEY,threshold)
    this
  }
  setThreshold(0.5)

  def getThreshold: Double= getValueParam(THRESHOLD_KEY).doubleValue()
  protected def thresholdFromUser:ParamFromUser={
    ParamFromUser.Double(ParamFromUser.variables.seed,s"请输入分类阈值#较高/低的分类阈值可以将更多的不够自信的数据分到负/正类中，从而增加正/负类的准确率，默认0.5").>=(0).<=(1).fix.addDefaultValue("0.5")
  }
}
