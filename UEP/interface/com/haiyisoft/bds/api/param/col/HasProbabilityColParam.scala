package com.haiyisoft.bds.api.param.col

import com.haiyisoft.bds.api.param.{ParamFromUser, WithParam}

/**
  * Created by XingxueDU on 2018/3/22.
  */
trait HasProbabilityColParam extends WithParam{
  final val PROBAB_COL_KEY = ParamFromUser.variables.probabilityColName

  def getProbabilityCol: String ={
    val res = $(PROBAB_COL_KEY)
    res
  }

  def hasProbabilityCol:Boolean ={
    $(PROBAB_COL_KEY)!=null
  }

  /**
    * 设置输出列
    *
    * @param name 输出列的列名
    */
  def setProbabilityCol(name: String): this.type = {
    addParam(PROBAB_COL_KEY,name)
    this
  }
  protected def probabilityColFromUser:ParamFromUser ={
    ParamFromUser.String(PROBAB_COL_KEY,"请设置预测概率列列名#probabilityCol，如果无需输出具体概率可留空")
  }

}
