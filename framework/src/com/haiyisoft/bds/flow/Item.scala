package com.haiyisoft.bds.flow

import scala.util.Try


/** 流程步骤的底层接口
  *
  * @tparam T 被包装的流程项，如一个[[com.haiyisoft.bds.api.action.Action]]
  *           或者一个[[com.haiyisoft.bds.api.data.DataProcessorInter]]
  * Created by XingxueDU on 2018/3/19.
  */
trait Item[T] extends Serializable {

  /**
    * 是否cache生成的DF
    */
  protected var cacheLevel:Int = Item.CACHE_IF_RERUN

  /**
    * 获取被包装的流程
    *
    * @return 被包装的流程
    */
  def get:T

  /**
    * 获取流程ID
    *
    * @return 流程ID
    */
  def getId:String

  /**
    * 通过给定Id查找以该节点为结尾，其之前流程中的对应id的[[Item]]
    *
    * @param id 待查寻的id
    * @return id为给定id的Item，如果不存在（即使流程未封口）则返回null
    */
  def search(id:String):Item[_] = {
    if(getId == id)this
    else{
      val res = Try{previous().search(id)}
      if(res.isFailure)null
      else res.get
    }
  }

  /**
    * 该流程的前一项（数据流入口）。如果该流程为集合节点（[[com.haiyisoft.bds.api.data.DataJoin]]）则需要特别注意
    *
    * @return 前一流程项
    */
  def previous():GettableItem[_]

  /**
    * 设置生成DF的缓存等级
    * @param cacheLevel 生成df的缓存等级
    * @param applyForSubitems 是否将等级设置应用于整个（之前）流程
    */
  def setCacheLevel(cacheLevel:Int= Item.CACHE_IF_RERUN, applyForSubitems:Boolean): this.type ={
    this.cacheLevel = cacheLevel
    if(applyForSubitems)previous().setCacheLevel(cacheLevel,applyForSubitems)
    this
  }

  /**
    * 流程的完整性检查
    * @return 所有上游缺失的流程项。如果流程完整则返回一个空Array
    */
  def checkIntegrity:Array[Item[_]]

  override def toString = "[flow]"+this.getClass.getName

  def getFlowPlan:String
}
object Item{
  //如果df被重复运用则cache
  val CACHE_IF_RERUN = 0
  //生成后自动cache
  val ALWAYS_CACHE = 1
  //从不cache
  val NEVER_CACHE =2
}