package com.haiyisoft.bds.api.data

import java.util.logging.Logger

import org.apache.spark.sql.DataFrame

/** 数据预处理阶段通用接口，用于数据的清理、挖掘、选择、变换等过程
  *
  * 其实际含义是：作为流程的中间部分，传入与传出均为DataFrame
  * Created by XingxueDU on 2018/3/15.
  *
  *
  */
trait DataTransformInter extends DataProcessorInter{

  /**
    * 变换的执行接口，从待变换的data转化为变换后的data
 *
    * @param data 待变换的表
    * @return 变换后的表
    */
  def transform(data:DataFrame): DataFrame

  override def getNumDfNeedInput:Int = 1

  override def process():DataFrame={
    val dataList = getDataList
    if(dataList.length!=1)throw new IllegalArgumentException(s"Input ${dataList.length} DataFrame into a DataTransformInter witch need only one")
    if(!checkNecessaryParam)Logger.getLogger(this.getClass.getName).warning("Miss some necessary params")
    transform(dataList.head)
  }
}
