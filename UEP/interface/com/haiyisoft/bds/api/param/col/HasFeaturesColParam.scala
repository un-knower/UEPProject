package com.haiyisoft.bds.api.param.col

import com.haiyisoft.bds.api.param.{ParamFromUser, WithParam}

import scala.collection.mutable.ArrayBuffer

/**
  * 传入列有features列。
  */
trait HasFeaturesColParam extends WithParam {
  final val FEATURES_COL_KEY = ParamFromUser.variables.featuresColName

  /**
    * 以列名的方式传入列。
    *
    * @param name 特征列列名
    */
  def setFeaturesCol(name: String): this.type = {
    addParam(FEATURES_COL_KEY, name)
    this
  }


  def getFeaturesCol: String = getStringParam(FEATURES_COL_KEY)

  def getInputColsImpl: String = getFeaturesCol

  override protected def getDefaultParamList: ArrayBuffer[ParamFromUser] = {
    val old = super.getDefaultParamList
    old += featuresColFromUser
    old
  }

  protected def featuresColFromUser: ParamFromUser = {
    ParamFromUser
      .String(ParamFromUser.variables.featuresColName, s"请输入传入特征列列名#featuresColName,数据类型spark.ml.Vector", notEmpty = true)
      .addDefaultValue("features")
      .setNeedSchema(true)
      .setShortName("特征列")
  }

}
