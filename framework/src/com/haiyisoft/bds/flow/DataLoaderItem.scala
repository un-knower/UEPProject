package com.haiyisoft.bds.flow

import com.haiyisoft.bds.api.data.DataLoadInter
import org.apache.spark.sql.DataFrame

/**
  * [[DataLoadInter]]的流程包装类
  *
  * @param dataLoad [[DataLoadInter]]类本身
  * @param id 流程id
  *
  * Created by XingxueDU on 2018/3/19.
  */
class DataLoaderItem[T<:DataLoadInter](private val dataLoad:T, private val id:String) extends GettableItem[T]{

  override def getFlowPlan: String = s"Start => ${dataLoad.getClass.getName.split('.').last}[$id]"

  /**
    * 流程的完整性检查
    *
    * @return 所有上游缺失的流程项。如果流程完整则返回一个空Array
    */
  override def checkIntegrity: Array[Item[_]] = Array()

  override protected def run: DataFrame = {
    get.getData
  }

  override def get: T = dataLoad

  override def getId: String = id

  override def previous(): GettableItem[_] = null

  override def search(id:String):Item[_] = {
    if(getId == id)this
    else null
  }

  override def toString:String = s"[flow]DataLoaderItem[$id]:$dataLoad"

}
object DataLoaderItem{
  def apply[T<:DataLoadInter](dataLoad:T,id:String):DataLoaderItem[T]=new DataLoaderItem(dataLoad,id)
}