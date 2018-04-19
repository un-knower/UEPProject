package com.haiyisoft.bds.api.data

import com.haiyisoft.bds.api.param.col.{HasMultipleInputParamByName, HasOutputParam}
import org.apache.spark.sql._

/**
  * Created by XingxueDU on 2018/3/19.
  */
trait DataJoin extends DataProcessorInter with HasMultipleInputParamByName with HasOutputParam{

  def join(fstTable:DataFrame,sndTable:DataFrame):DataFrame

  override def getNumDfNeedInput:Int = 2

  override def process():DataFrame={
    val dataList = getDataList
    if(dataList.length!=2)throw new IllegalArgumentException(s"Input ${dataList.length} DataFrame into a DataJoin witch need two")
    val Array(fst:DataFrame,snd:DataFrame) = dataList
    join(fst,snd)
  }
}
