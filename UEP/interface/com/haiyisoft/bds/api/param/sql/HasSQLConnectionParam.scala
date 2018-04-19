package com.haiyisoft.bds.api.param.sql

/**
  * Created by XingxueDU on 2018/3/26.
  */
trait HasSQLConnectionParam extends HasSQLPropertiesParam{
  private[bds] var localConnect: java.sql.Connection = _
  def isConnected:Boolean = localConnect!=null && !localConnect.isClosed
  def connect():this.type
  def disconnect:this.type ={
    if(isConnected){
      localConnect.close()
    }
    localConnect = null
    this
  }
  def getConnection: java.sql.Connection = {
    if(!isConnected)connect()
    localConnect
  }
}
