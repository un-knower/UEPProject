package com.haiyisoft.bds.api.data

import com.haiyisoft.bds.api.param.WithParam
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

/**
  * Created by XingxueDU on 2018/3/19.
  */
trait DataProcessorInter extends WithParam with Serializable{

  private val inData = new ArrayBuffer[DataFrame](2)

  def addData(in:DataFrame*):this.type={
    inData ++= in
    this
  }

  def clearDataList():this.type={
    inData.clear()
    this
  }

  protected def getDataList = inData.toArray

  /**
    * 获取该流程需要传入的df数量
    * @return
    */
  def getNumDfNeedInput:Int

  /**
    * 获取transform类型
    * @return
    */

  def process():DataFrame
}
