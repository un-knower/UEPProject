package com.haiyisoft.bds.plugins.action

import com.haiyisoft.bds.api.action.DataFrameAction
import com.haiyisoft.bds.api.param.ParamFromUser
import com.haiyisoft.bds.api.param.save.HasSaveDataFrame2ParquetParam
import org.apache.spark.sql._

/**
  * Created by XingxueDU on 2018/3/21.
  */
class DataFrameSaveToParquetAction extends DataFrameAction with HasSaveDataFrame2ParquetParam{

  override def run(data:DataFrame):Unit={
    save(data)
  }

  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  override def getNecessaryParamList: Array[ParamFromUser] = {
    getDefaultParamList.toArray
  }

  override def getPluginName: String = "保存parquet"
}