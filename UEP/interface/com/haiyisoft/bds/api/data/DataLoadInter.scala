package com.haiyisoft.bds.api.data

import com.haiyisoft.bds.api.param.HasLoadParam
import org.apache.spark.sql.DataFrame

/** 数据获取（数据集成）的通用接口，用于计算流程的第一步
  * Created by XingxueDU on 2018/3/15.
  *
  * @version 0.0.1 build 2
  */
trait DataLoadInter extends HasLoadParam{
  def getData: DataFrame

}
