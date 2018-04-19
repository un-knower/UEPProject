package com.haiyisoft.bds.plugins.process

import com.haiyisoft.bds.api.data.DataTransformInter
import com.haiyisoft.bds.api.param.ParamFromUser
import org.apache.spark.sql.DataFrame

/**
  * Created by XingxueDU on 2018/3/22.
  */
class ShowDFTransformer extends DataTransformInter{
  /**
    * 变换的执行接口，从待变换的data转化为变换后的data
    *
    * @param data 待变换的表
    * @return 变换后的表
    */
  override def transform(data: DataFrame): DataFrame = {
    data.show()
    data
  }

  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  override def getNecessaryParamList: Array[ParamFromUser] = {
    Array()
  }

  override def getPluginName: String = "[DEBUG]打印表格"
}
