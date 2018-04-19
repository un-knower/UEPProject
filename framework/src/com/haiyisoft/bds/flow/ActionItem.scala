package com.haiyisoft.bds.flow

import com.haiyisoft.bds.api.action.Action

/** 一个流程的入口，也就是流程的终点，[[Action]]的包装类
  *
  *
  * @param action 被包装的[[Action]]类本身
  * @param id 流程id
  * Created by XingxueDU on 2018/3/19.
  */
class ActionItem[T<:Action[_]](private val action:T,private val id:String) extends Item[T]{

  override def getFlowPlan: String = s"${previous().getFlowPlan} -> ${action.getClass.getName.split('.').last}[$id] => End"

  /**
    * 流程的完整性检查
    *
    * @return 所有上游缺失的流程项。如果流程完整则返回一个空Array
    */
  override def checkIntegrity: Array[Item[_]] = {
    if(previous()==null)Array(this) else previous().checkIntegrity
  }

  //上一步的指针
  private var last:GettableItem[_] = null

  /**
    * 从上一步获取数据并执行
    * 由于action作为流程终点，该方法会处理整个流程
    */
  def run():Unit = {
    get.run(previous().runAndGet)
  }

  override def get: T = action

  override def getId: String = id

  override def previous(): GettableItem[_] = {
    if(this.last==null)throw new NullPointerException(s"Haven't set the previous for your action[id = $id]")
    last
  }

  /**
    * 设置Action上一步的包装
    *
    * @param it 上一步行为的包装类
    */
  def setPrevious(it:GettableItem[_]):this.type ={
    this.last = it
    this
  }

  override def toString:String = s"[flow]ActionItem[$id]:$action"

}
object ActionItem{
  def apply[T<:Action[_]](action:T,id:String):ActionItem[T]=new ActionItem(action,id)
}