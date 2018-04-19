package com.haiyisoft.bds.flow

import com.haiyisoft.bds.api.data.DataProcessorInter
import org.apache.spark.sql.DataFrame

/** [[DataProcessorInter]]的流程包装类
  *
  * @param processor DataProcessorInter本身
  * @param id 流程id
  *
  * Created by XingxueDU on 2018/3/19.
  */
class DataProcessorItem[T<:DataProcessorInter](private val processor:T,private val id:String) extends GettableItem[T]{

  override def getFlowPlan: String = {
    if(processor.getNumDfNeedInput>1){
      val lastList = previousList()
      var tp = 0
      lastList.map{t=>
        tp += 1
        t.getFlowPlan+s" => ####$id[$tp]####"
      }.mkString("\n")+s"####$id[${lastList.indices.map(_+1).mkString(",")}]#### => ${processor.getClass.getName.split('.').last}[$id]"
    }else
      s"${previous().getFlowPlan} -> ${processor.getClass.getName.split('.').last}[$id]"
  }

  private val last:Array[GettableItem[_]] = new Array[GettableItem[_]](processor.getNumDfNeedInput)

  /**
    * 流程的完整性检查
    *
    * @return 所有上游缺失的流程项。如果流程完整则返回一个空Array
    */
  override def checkIntegrity: Array[Item[_]] = {
    var res = Array[Item[_]]()
    if(last.contains(null)) res :+= this
    last.foreach{k=>
      if(k!=null)res++=k.checkIntegrity
    }
    res
  }

  override def run: DataFrame = {
    val datas = last.map(_.runAndGet)
    processor.addData(datas:_*)
    processor.process()
  }

  override def get: T = this.processor

  override def getId: String = id

  override def previous(): GettableItem[_] = {
    if(processor.getNumDfNeedInput>1){
      new MultipleGettableItem(last,this)
    }else last.head
  }

  /**
    * 获取全部上游流程列表。如果该流程有复数传入则应该使用此类
    *
    * @return 上游流程列表
    */
  def previousList(): Array[GettableItem[_]] = this.last

  /**
    * 在idx (0 until length)位置放置流程
    *
    * @param idx 位置索引
    * @param it 流程类
    */
  def addPrevious(idx:Int,it:GettableItem[_]):this.type ={
    if(idx>=processor.getNumDfNeedInput)throw new IllegalArgumentException(s"try to met item idx=$idx[$it] as previous but this[${processor.getClass.getName.split('.').last}] need only ${processor.getNumDfNeedInput} previous item${if(processor.getNumDfNeedInput>1)"s"}")
    this.last(idx) = it
    this
  }

  /**
    * 一次性添加全部上游流程
    *
    * @param it 上游流程类
    */
  def addAllPrevious(it:Array[GettableItem[_]]):this.type ={
    it.copyToArray(this.last)
    this
  }

  /**
    * 获取被包装流程的上游流程个数，即该步骤有多少DataFrame传入
    *
    * @return
    */
  def getPreviousSize:Int = processor.getNumDfNeedInput


  override def equals(obj: scala.Any): Boolean = {
    obj.isInstanceOf[this.type]&&{
      val oth = obj.asInstanceOf[this.type]
      oth.processor == this.processor &&
      oth.id == this.id
    }
  }

  override def toString:String = s"[flow]DataProcessorItem[$id]:$processor"
}
object DataProcessorItem{
  def apply[T<:DataProcessorInter](processor:T,id:String):DataProcessorItem[T]=new DataProcessorItem(processor,id)
}