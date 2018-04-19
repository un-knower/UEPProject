package com.haiyisoft.bds.plugins.process.base

import com.haiyisoft.bds.api.data.Converter
import com.haiyisoft.bds.api.data.trans.DataProcessInter
import com.haiyisoft.bds.api.param.ParamFromUser
import com.haiyisoft.bds.api.param.col.HasMultipleInputParamByName
import org.apache.spark.sql.DataFrame

//TODO col sql 支持
/**
  * Created by XingxueDU on 2018/3/27.
  */
class SelectByCols extends DataProcessInter with HasMultipleInputParamByName {
  /**
    * 变换的执行接口，从待变换的data转化为变换后的data
    *
    * @param data 待变换的表
    * @return 变换后的表
    */
  override def transform(data: DataFrame): DataFrame = {
    val cols = getInputCols
    data.select(cols: _*)
  }

  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  override def getNecessaryParamList: Array[ParamFromUser] = {
    Array(multipleInputColFromUser()
      .setNeedSchema(true)
      .setDescription("请输入要保留的列列名#SQL:SELECT [*] FROM TABLE")
      .setShortName("选择列"))

  }

  override def getPluginName: String = "根据列名过滤"

  override def getTransType: Converter.Value = Converter.SELECT

  def schemaCols: Array[String] = getInputColsImpl
}
