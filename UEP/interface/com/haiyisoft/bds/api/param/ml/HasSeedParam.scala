package com.haiyisoft.bds.api.param.ml

import com.haiyisoft.bds.api.param.{ParamFromUser, WithParam}

/**
  * Created by XingxueDU on 2018/3/22.
  */
trait HasSeedParam extends WithParam{
  final val SEED_NUM_KEY = ParamFromUser.variables.seed
  /**
    * 设置随机数种子。
    *
    * @param seed 特征列列名
    */
  def setSeed(seed:Long): this.type={
    addParam(SEED_NUM_KEY,seed)
    this
  }
  setSeed(0)

  def hasSeed:Boolean = getValueParam(SEED_NUM_KEY)>0
  def getSeed: Long= getValueParam(SEED_NUM_KEY).longValue()
  protected def seedFromUser:ParamFromUser={
    ParamFromUser.Long(ParamFromUser.variables.seed,s"请输入随机数种子#相同的随机数种子可能固定算法结果。输入0则使用随机的种子").>=("0").fix.addDefaultValue("0")
  }
}
