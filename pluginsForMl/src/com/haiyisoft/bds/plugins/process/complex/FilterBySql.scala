package com.haiyisoft.bds.plugins.process.complex

import com.haiyisoft.bds.api.data.trans.DataProcessInter
import com.haiyisoft.bds.api.param.ParamFromUser
import org.apache.spark.sql.DataFrame

/**
  * Created by Namhwik on 2018/3/19.
  */
class FilterBySql extends DataProcessInter {
  val SQL_KEY = "filterSql"

  override def transform(data: DataFrame): DataFrame = {
    val filterStr = getStringParam(SQL_KEY)
    if (filterStr == null)
      throw new NullPointerException("unset Necessary Param")
    else
      data.filter(filterStr.toString)
  }

  override def getNecessaryParamList: Array[ParamFromUser] = {
    Array(ParamFromUser
      .String(SQL_KEY, "请输入过滤表达式#sql表达式,如[列名]>'1'等")
      .setShortName("过滤表达式")
    )
  }

  override def getPluginName: String = "SQL过滤器"
}
