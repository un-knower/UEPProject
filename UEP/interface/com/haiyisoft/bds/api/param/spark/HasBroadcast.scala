package com.haiyisoft.bds.api.param.spark

import com.haiyisoft.bds.api.param.{ParamFromUser, WithParam}
import org.apache.spark.broadcast.Broadcast


/**
  * Created by XingxueDU on 2018/3/26.
  */
trait HasBroadcast[T] extends WithParam{
  val BROADCAST_KEY ="data.broadcast"

  def getNumBroadcast:Int

  protected def getBroadcastKeyById(idx:Int= 0):String={
    if(getNumBroadcast<=1)BROADCAST_KEY else BROADCAST_KEY+"."+idx
  }

  def broadcastFromUser(idx:Int= 0):ParamFromUser ={
    val key = getBroadcastKeyById(idx)
    ParamFromUser.String(key,"请输入广播变量",notEmpty = true)
      .setTypeName("broadcast")
  }
  def getBroadcast[W](idx:Int = 0):Broadcast[W]={
    import scala.collection.JavaConversions._
    val key = getBroadcastKeyById(idx)
    getParam(key).asInstanceOf
  }
  def setBroadcast[W](broadcast:Broadcast[W],idx:Int = 0): this.type ={
    val key = getBroadcastKeyById(idx)
    addParam(key,broadcast)
  }
}
trait HasMapStringBroadcast extends HasBroadcast[Map[String,String]]{

  def getMapStringBroadcast(idx:Int = 0):Broadcast[Map[String,String]]=getBroadcast(idx)
}